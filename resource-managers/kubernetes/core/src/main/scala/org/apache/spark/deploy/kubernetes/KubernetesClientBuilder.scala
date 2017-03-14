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
package org.apache.spark.deploy.kubernetes

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import io.fabric8.kubernetes.client.{Config, ConfigBuilder, DefaultKubernetesClient}

import org.apache.spark.deploy.kubernetes.constants._

private[spark] object KubernetesClientBuilder {
  private val SERVICE_ACCOUNT_TOKEN = new File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)
  private val SERVICE_ACCOUNT_CA_CERT = new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)
  private val MOUNTED_CREDENTIALS_BASE_DIR = new File(
    DRIVER_CONTAINER_KUBERNETES_CREDENTIALS_SECRETS_BASE_DIR)
  private val MOUNTED_TOKEN = new File(MOUNTED_CREDENTIALS_BASE_DIR,
    DRIVER_CONTAINER_OAUTH_TOKEN_SECRET_NAME)
  private val MOUNTED_CA_CERT = new File(MOUNTED_CREDENTIALS_BASE_DIR,
    DRIVER_CONTAINER_CA_CERT_FILE_SECRET_NAME)
  private val MOUNTED_CLIENT_KEY = new File(MOUNTED_CREDENTIALS_BASE_DIR,
    DRIVER_CONTAINER_CLIENT_KEY_FILE_SECRET_NAME)
  private val MOUNTED_CLIENT_CERT = new File(MOUNTED_CREDENTIALS_BASE_DIR,
    DRIVER_CONTAINER_CLIENT_CERT_FILE_SECRET_NAME)

  /**
   * Creates a {@link KubernetesClient}, expecting to be from
   * within the context of a pod. When doing so, credentials files
   * are picked up from canonical locations, as they are injected
   * into the pod's disk space.
   */
  def buildFromWithinPod(kubernetesNamespace: String): DefaultKubernetesClient = {
    var clientConfigBuilder = new ConfigBuilder()
      .withApiVersion("v1")
      .withMasterUrl(KUBERNETES_MASTER_INTERNAL_URL)
      .withNamespace(kubernetesNamespace)

    if (MOUNTED_TOKEN.isFile ||
        MOUNTED_CA_CERT.isFile ||
        MOUNTED_CLIENT_KEY.isFile ||
        MOUNTED_CLIENT_CERT.isFile) {
      if (MOUNTED_TOKEN.isFile) {
        clientConfigBuilder = clientConfigBuilder.withOauthToken(
          Files.toString(MOUNTED_TOKEN, Charsets.UTF_8))
      }
      if (MOUNTED_CA_CERT.isFile) {
        clientConfigBuilder = clientConfigBuilder.withCaCertFile(MOUNTED_CA_CERT.getAbsolutePath)
      }
      if (MOUNTED_CLIENT_KEY.isFile) {
        clientConfigBuilder = clientConfigBuilder.withClientKeyFile(
          MOUNTED_CLIENT_KEY.getAbsolutePath)
      }
      if (MOUNTED_CLIENT_CERT.isFile) {
        clientConfigBuilder = clientConfigBuilder.withClientCertFile(
          MOUNTED_CLIENT_CERT.getAbsolutePath)
      }
    } else {
      if (SERVICE_ACCOUNT_CA_CERT.isFile) {
        clientConfigBuilder = clientConfigBuilder.withCaCertFile(
          SERVICE_ACCOUNT_CA_CERT.getAbsolutePath)
      }

      if (SERVICE_ACCOUNT_TOKEN.isFile) {
        clientConfigBuilder = clientConfigBuilder.withOauthToken(
          Files.toString(SERVICE_ACCOUNT_TOKEN, Charsets.UTF_8))
      }
    }
    new DefaultKubernetesClient(clientConfigBuilder.build)
  }
}
