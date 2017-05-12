/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.scheduler.cluster.kubernetes

import java.io.Closeable
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.apache.spark.{SparkContext, SparkException}

import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.rpc.{RpcAddress, RpcEndpointAddress, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class KubernetesClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    val sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  import KubernetesClusterSchedulerBackend._

  private val RUNNING_EXECUTOR_PODS_LOCK = new Object
  private val runningExecutorsToPods = new mutable.HashMap[String, Pod] // Indexed by executor IDs.
  private val runningPodsToExecutors = new mutable.HashMap[Pod, String] // Indexed by executor Pods.
  private val EXECUTOR_PODS_BY_IPS_LOCK = new Object
  private val executorPodsByIPs = new mutable.HashMap[String, Pod] // Indexed by executor IP addrs.
  private val FAILED_PODS_LOCK = new Object
  private val failedPods = new mutable.HashMap[String, ExecutorLossReason] // Indexed by pod names.
  private val EXECUTORS_TO_REMOVE_LOCK = new Object
  private val executorsToRemove = new mutable.HashSet[String]

  private val executorDockerImage = conf.get(EXECUTOR_DOCKER_IMAGE)
  private val kubernetesNamespace = conf.get(KUBERNETES_NAMESPACE)
  private val executorPort = conf.getInt("spark.executor.port", DEFAULT_STATIC_PORT)
  private val blockmanagerPort = conf
    .getInt("spark.blockmanager.port", DEFAULT_BLOCKMANAGER_PORT)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)
    .getOrElse(
      throw new SparkException("Must specify the driver pod name"))

  private val executorMemoryMb = conf.get(org.apache.spark.internal.config.EXECUTOR_MEMORY)
  private val executorMemoryString = conf.get(
    org.apache.spark.internal.config.EXECUTOR_MEMORY.key,
    org.apache.spark.internal.config.EXECUTOR_MEMORY.defaultValueString)

  private val memoryOverheadMb = conf
    .get(KUBERNETES_EXECUTOR_MEMORY_OVERHEAD)
    .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * executorMemoryMb).toInt,
      MEMORY_OVERHEAD_MIN))
  private val executorMemoryWithOverhead = executorMemoryMb + memoryOverheadMb

  private val executorCores = conf.getOption("spark.executor.cores").getOrElse("1")

  private implicit val requestExecutorContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("kubernetes-executor-requests"))

  private val kubernetesClient = new KubernetesClientBuilder(conf, kubernetesNamespace)
    .buildFromWithinPod()

  private val driverPod = try {
    kubernetesClient.pods().inNamespace(kubernetesNamespace).
      withName(kubernetesDriverPodName).get()
  } catch {
    case throwable: Throwable =>
      logError(s"Executor cannot find driver pod.", throwable)
      throw new SparkException(s"Executor cannot find driver pod", throwable)
  }

  override val minRegisteredRatio: Double =
    if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  private val executorWatchResource = new AtomicReference[Closeable]
  private val executorCleanupScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    "executor-recovery-worker")
  protected var totalExpectedExecutors = new AtomicInteger(0)


  private val driverUrl = RpcEndpointAddress(
    sc.getConf.get("spark.driver.host"),
    sc.getConf.getInt("spark.driver.port", DEFAULT_DRIVER_PORT),
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

  private val initialExecutors = getInitialTargetExecutorNumber()

  private def getInitialTargetExecutorNumber(defaultNumExecutors: Int = 1): Int = {
    if (Utils.isDynamicAllocationEnabled(conf)) {
      val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", 0)
      val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)
      val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", 1)
      require(initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number " +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    } else {
      conf.getInt("spark.executor.instances", defaultNumExecutors)
    }
  }

  override def applicationId(): String = conf.get("spark.app.id", super.applicationId())

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  override def start(): Unit = {
    super.start()
    executorWatchResource.set(kubernetesClient.pods().withLabel(SPARK_APP_ID_LABEL, applicationId())
      .watch(new ExecutorPodsWatcher()))
    if (!Utils.isDynamicAllocationEnabled(sc.conf)) {
      doRequestTotalExecutors(initialExecutors)
    }
    executorCleanupScheduler.scheduleWithFixedDelay(executorRecoveryRunnable, 0,
      TimeUnit.SECONDS.toMillis(5), TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = {
    // TODO investigate why Utils.tryLogNonFatalError() doesn't work in this context.
    // When using Utils.tryLogNonFatalError some of the code fails but without any logs or
    // indication as to why.
    try {
      RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        runningExecutorsToPods.values.foreach(kubernetesClient.pods().delete(_))
        runningPodsToExecutors.clear()
      }
      EXECUTOR_PODS_BY_IPS_LOCK.synchronized {
        executorPodsByIPs.clear()
      }
      val resource = executorWatchResource.getAndSet(null)
      if (resource != null) {
        resource.close()
      }
    } catch {
      case e: Throwable => logError("Uncaught exception while shutting down controllers.", e)
    }
    try {
      logInfo("Closing kubernetes client")
      kubernetesClient.close()
    } catch {
      case e: Throwable => logError("Uncaught exception closing Kubernetes client.", e)
    }
    executorCleanupScheduler.shutdown()
    super.stop()
  }

  private def allocateNewExecutorPod(): (String, Pod) = {
    val executorId = EXECUTOR_ID_COUNTER.incrementAndGet().toString
    val name = s"${applicationId()}-exec-$executorId"

    // hostname must be no longer than 63 characters, so take the last 63 characters of the pod
    // name as the hostname.  This preserves uniqueness since the end of name contains
    // executorId and applicationId
    val hostname = name.substring(Math.max(0, name.length - 63))

    val selectors = Map(SPARK_EXECUTOR_ID_LABEL -> executorId,
      SPARK_APP_ID_LABEL -> applicationId()).asJava
    val executorMemoryQuantity = new QuantityBuilder(false)
      .withAmount(s"${executorMemoryMb}M")
      .build()
    val executorMemoryLimitQuantity = new QuantityBuilder(false)
      .withAmount(s"${executorMemoryWithOverhead}M")
      .build()
    val executorCpuQuantity = new QuantityBuilder(false)
      .withAmount(executorCores)
      .build()
    val requiredEnv = Seq(
      (ENV_EXECUTOR_PORT, executorPort.toString),
      (ENV_DRIVER_URL, driverUrl),
      (ENV_EXECUTOR_CORES, executorCores),
      (ENV_EXECUTOR_MEMORY, executorMemoryString),
      (ENV_APPLICATION_ID, applicationId()),
      (ENV_EXECUTOR_ID, executorId))
      .map(env => new EnvVarBuilder()
        .withName(env._1)
        .withValue(env._2)
        .build()
      ) ++ Seq(
      new EnvVarBuilder()
        .withName(ENV_EXECUTOR_POD_IP)
        .withValueFrom(new EnvVarSourceBuilder()
          .withNewFieldRef("v1", "status.podIP")
          .build())
        .build()
      )
    val requiredPorts = Seq(
      (EXECUTOR_PORT_NAME, executorPort),
      (BLOCK_MANAGER_PORT_NAME, blockmanagerPort))
      .map(port => {
        new ContainerPortBuilder()
          .withName(port._1)
          .withContainerPort(port._2)
          .build()
      })
    try {
      (executorId, kubernetesClient.pods().createNew()
        .withNewMetadata()
          .withName(name)
          .withLabels(selectors)
          .withOwnerReferences()
          .addNewOwnerReference()
            .withController(true)
            .withApiVersion(driverPod.getApiVersion)
            .withKind(driverPod.getKind)
            .withName(driverPod.getMetadata.getName)
            .withUid(driverPod.getMetadata.getUid)
          .endOwnerReference()
        .endMetadata()
        .withNewSpec()
          .withHostname(hostname)
          .addNewContainer()
            .withName(s"executor")
            .withImage(executorDockerImage)
            .withImagePullPolicy("IfNotPresent")
            .withNewResources()
              .addToRequests("memory", executorMemoryQuantity)
              .addToLimits("memory", executorMemoryLimitQuantity)
              .addToRequests("cpu", executorCpuQuantity)
              .addToLimits("cpu", executorCpuQuantity)
              .endResources()
            .withEnv(requiredEnv.asJava)
            .withPorts(requiredPorts.asJava)
            .endContainer()
          .endSpec()
        .done())
    } catch {
      case throwable: Throwable =>
        logError("Failed to allocate executor pod.", throwable)
        throw throwable
    }
  }

  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = Future[Boolean] {
    RUNNING_EXECUTOR_PODS_LOCK.synchronized {
      if (requestedTotal > totalExpectedExecutors.get) {
        logInfo(s"Requesting ${
          requestedTotal - totalExpectedExecutors.get
        }"
          + s" additional executors, expecting total $requestedTotal and currently" +
          s" expected ${totalExpectedExecutors.get}")
        for (i <- 0 until (requestedTotal - totalExpectedExecutors.get)) {
          val (executorId, pod) = allocateNewExecutorPod()
          runningExecutorsToPods.put(executorId, pod)
          runningPodsToExecutors.put(pod, executorId)
        }
      }
      totalExpectedExecutors.set(requestedTotal)
    }
    true
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = Future[Boolean] {
    RUNNING_EXECUTOR_PODS_LOCK.synchronized {
      for (executor <- executorIds) {
        runningExecutorsToPods.remove(executor) match {
          case Some(pod) =>
            kubernetesClient.pods().delete(pod)
            runningPodsToExecutors.remove(pod)
          case None => logWarning(s"Unable to remove pod for unknown executor $executor")
        }
      }
    }
    true
  }

  def getExecutorPodByIP(podIP: String): Option[Pod] = {
    EXECUTOR_PODS_BY_IPS_LOCK.synchronized {
      executorPodsByIPs.get(podIP)
    }
  }

  private class ExecutorPodsWatcher extends Watcher[Pod] {

    private val DEFAULT_CONTAINER_FAILURE_EXIT_STATUS = -1

    override def eventReceived(action: Action, pod: Pod): Unit = {

      if (action == Action.MODIFIED && pod.getStatus.getPhase == "Running"
        && pod.getMetadata.getDeletionTimestamp == null) {
        val podIP = pod.getStatus.getPodIP
        val clusterNodeName = pod.getSpec.getNodeName
        logDebug(s"Executor pod $pod ready, launched at $clusterNodeName as IP $podIP.")
        EXECUTOR_PODS_BY_IPS_LOCK.synchronized {
          executorPodsByIPs += ((podIP, pod))
        }
      } else if ((action == Action.MODIFIED && pod.getMetadata.getDeletionTimestamp != null) ||
        action == Action.DELETED || action == Action.ERROR) {
        val podName = pod.getMetadata.getName
        val podIP = pod.getStatus.getPodIP
        logDebug(s"Executor pod $podName at IP $podIP was at $action.")
        if (podIP != null) {
          EXECUTOR_PODS_BY_IPS_LOCK.synchronized {
            executorPodsByIPs -= podIP
          }
        }
        if (action == Action.ERROR) {
          logInfo(s"Received pod $podName exited event. Reason: " + pod.getStatus.getReason)
          handleErroredPod(pod)
        }
        else if (action == Action.DELETED) {
          logInfo(s"Received delete pod $podName event. Reason: " + pod.getStatus.getReason)
          handleDeletedPod(pod)
        }
      }
    }

    override def onClose(cause: KubernetesClientException): Unit = {
      logDebug("Executor pod watch closed.", cause)
    }

    def getContainerExitStatus(pod: Pod): Int = {
      val containerStatuses = pod.getStatus.getContainerStatuses
      if (!containerStatuses.isEmpty) {
        return getContainerExitStatus(containerStatuses.get(0))
      }
      DEFAULT_CONTAINER_FAILURE_EXIT_STATUS
    }

    def getContainerExitStatus(containerStatus: ContainerStatus): Int = {
      containerStatus.getState.getTerminated.getExitCode.intValue
    }

    def handleErroredPod(pod: Pod): Unit = {
      def isPodAlreadyReleased(pod: Pod): Boolean = {
        RUNNING_EXECUTOR_PODS_LOCK.synchronized {
          runningPodsToExecutors.keySet.foreach(runningPod =>
            if (runningPod.getMetadata.getName == pod.getMetadata.getName) {
              return false
            }
          )
        }
        true
      }
      val alreadyReleased = isPodAlreadyReleased(pod)
      val containerExitStatus = getContainerExitStatus(pod)
      // container was probably actively killed by the driver.
      val exitReason = if (alreadyReleased) {
          ExecutorExited(containerExitStatus, exitCausedByApp = false,
            s"Container in pod " + pod.getMetadata.getName +
              " exited from explicit termination request.")
        } else {
          val containerExitReason = containerExitStatus match {
            case VMEM_EXCEEDED_EXIT_CODE | PMEM_EXCEEDED_EXIT_CODE =>
              memLimitExceededLogMessage(pod.getStatus.getReason)
            case _ =>
              // Here we can't be sure that that exit was caused by the application but this seems
              // to be the right default since we know the pod was not explicitly deleted by
              // the user.
              "Pod exited with following container exit status code " + containerExitStatus
          }
          ExecutorExited(containerExitStatus, exitCausedByApp = true, containerExitReason)
        }
      FAILED_PODS_LOCK.synchronized {
        failedPods.put(pod.getMetadata.getName, exitReason)
      }
    }

    def handleDeletedPod(pod: Pod): Unit = {
      val exitReason = ExecutorExited(getContainerExitStatus(pod), exitCausedByApp = false,
        "Pod " + pod.getMetadata.getName + " deleted or lost.")
      FAILED_PODS_LOCK.synchronized {
        failedPods.put(pod.getMetadata.getName, exitReason)
      }
    }
  }

  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new KubernetesDriverEndpoint(rpcEnv, properties)
  }

  private class KubernetesDriverEndpoint(rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends DriverEndpoint(rpcEnv, sparkProperties) {

    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      addressToExecutorId.get(rpcAddress).foreach { executorId =>
        if (disableExecutor(executorId)) {
          EXECUTORS_TO_REMOVE_LOCK.synchronized {
            executorsToRemove.add(executorId)
          }
        }
      }
    }
  }

  private val executorRecoveryRunnable: Runnable = new Runnable {

    private val MAX_EXECUTOR_LOST_REASON_CHECKS = 10
    private val executorsToRecover = new mutable.HashSet[String]
    // Maintains a map of executor id to count of checks performed to learn the loss reason
    // for an executor.
    private val executorReasonChecks = new mutable.HashMap[String, Int]

    override def run(): Unit = removeFailedAndRequestNewExecutors()

    def removeFailedAndRequestNewExecutors(): Unit = {
      val localRunningExecutorsToPods = RUNNING_EXECUTOR_PODS_LOCK.synchronized {
        runningExecutorsToPods.toMap
      }
      val localFailedPods = FAILED_PODS_LOCK.synchronized {
        failedPods.toMap
      }
      val localExecutorsToRemove = EXECUTORS_TO_REMOVE_LOCK.synchronized {
        executorsToRemove.toSet
      }
      localExecutorsToRemove.foreach { case (executorId) =>
        localRunningExecutorsToPods.get(executorId) match {
          case Some(pod) =>
            localFailedPods.get(pod.getMetadata.getName) match {
              case Some(executorExited: ExecutorExited) =>
                logDebug(s"Removing executor $executorId with loss reason "
                  + executorExited.message)
                removeExecutor(executorId, executorExited)
                if (!executorExited.exitCausedByApp) {
                  executorsToRecover.add(executorId)
                }
              case None =>
                removeExecutorOrIncrementLossReasonCheckCount(executorId)
            }
          case None =>
            removeExecutorOrIncrementLossReasonCheckCount(executorId)
        }
      }
      executorsToRecover.foreach(executorId =>
        EXECUTORS_TO_REMOVE_LOCK.synchronized {
          executorsToRemove -= executorId
          executorReasonChecks -= executorId
        }
      )
      if (executorsToRecover.nonEmpty) {
        requestExecutors(executorsToRecover.size)
      }
      executorsToRecover.clear()
    }


    def removeExecutorOrIncrementLossReasonCheckCount(executorId: String): Unit = {
      val reasonCheckCount = executorReasonChecks.getOrElse(executorId, 0)
      if (reasonCheckCount > MAX_EXECUTOR_LOST_REASON_CHECKS) {
        removeExecutor(executorId, SlaveLost("Executor lost for unknown reasons"))
        executorsToRecover.add(executorId)
        executorReasonChecks -= executorId
      } else {
        executorReasonChecks.put(executorId, reasonCheckCount + 1)
      }
    }
  }
}

private object KubernetesClusterSchedulerBackend {
  private val DEFAULT_STATIC_PORT = 10000
  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)
  private val VMEM_EXCEEDED_EXIT_CODE = -103
  private val PMEM_EXCEEDED_EXIT_CODE = -104

  def memLimitExceededLogMessage(diagnostics: String): String = {
    s"Pod/Container killed for exceeding memory limits. $diagnostics" +
      " Consider boosting spark executor memory overhead."
  }
}

