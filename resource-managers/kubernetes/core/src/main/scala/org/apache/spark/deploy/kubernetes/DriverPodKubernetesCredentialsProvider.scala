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
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{Secret, SecretBuilder, Volume, VolumeBuilder, VolumeMount, VolumeMountBuilder}
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.config.OptionalConfigEntry

private[spark] case class DriverPodKubernetesCredentials(
  credentialsSecret: Secret,
  credentialsSecretVolume: Volume,
  credentialsSecretVolumeMount: VolumeMount)

private[spark] class DriverPodKubernetesCredentialsProvider(
    sparkConf: SparkConf,
    kubernetesAppId: String) {

  def getDriverPodKubernetesCredentials(): DriverPodKubernetesCredentials = {
    val oauthTokenSecretMapping = sparkConf
      .get(KUBERNETES_DRIVER_OAUTH_TOKEN)
      .map(token => (DRIVER_CONTAINER_OAUTH_TOKEN_SECRET_NAME,
         BaseEncoding.base64().encode(token.getBytes(Charsets.UTF_8))))
    val caCertSecretMapping = convertFileConfToSecretMapping(KUBERNETES_DRIVER_CA_CERT_FILE,
      DRIVER_CONTAINER_CA_CERT_FILE_SECRET_NAME)
    val clientKeyFileSecretMapping = convertFileConfToSecretMapping(
      KUBERNETES_DRIVER_CLIENT_KEY_FILE, DRIVER_CONTAINER_CLIENT_KEY_FILE_SECRET_NAME)
    val clientCertFileSecretMapping = convertFileConfToSecretMapping(
      KUBERNETES_DRIVER_CLIENT_CERT_FILE, DRIVER_CONTAINER_CLIENT_CERT_FILE_SECRET_NAME)
    val secretData = (oauthTokenSecretMapping ++
      caCertSecretMapping ++
      clientKeyFileSecretMapping ++
      clientCertFileSecretMapping).toMap
    val credentialsSecret = new SecretBuilder()
      .withNewMetadata()
        .withName(s"$DRIVER_CONTAINER_KUBERNETES_CREDENTIALS_SECRET_NAME-$kubernetesAppId")
        .endMetadata()
      .withData(secretData.asJava)
      .build()
    val credentialsSecretVolume = new VolumeBuilder()
      .withName(DRIVER_CONTAINER_KUBERNETES_CREDENTIALS_VOLUME_NAME)
      .withNewSecret()
        .withSecretName(credentialsSecret.getMetadata.getName)
        .endSecret()
      .build()
    val credentialsSecretVolumeMount = new VolumeMountBuilder()
      .withName(credentialsSecretVolume.getName)
      .withReadOnly(true)
      .withMountPath(DRIVER_CONTAINER_KUBERNETES_CREDENTIALS_SECRETS_BASE_DIR)
      .build()
    // Cannot use both service account and mounted secrets
    sparkConf.get(KUBERNETES_SERVICE_ACCOUNT_NAME).foreach { _ =>
      require(oauthTokenSecretMapping.isEmpty,
        "Cannot specify both a service account and a driver pod OAuth token.")
      require(caCertSecretMapping.isEmpty,
        "Cannot specify both a service account and a driver pod CA cert file.")
      require(clientKeyFileSecretMapping.isEmpty,
        "Cannot specify both a service account and a driver pod client key file.")
      require(clientCertFileSecretMapping.isEmpty,
        "Cannot specify both a service account and a driver pod client cert file.")
    }
    DriverPodKubernetesCredentials(
      credentialsSecret,
      credentialsSecretVolume,
      credentialsSecretVolumeMount)
  }

  private def convertFileConfToSecretMapping(
      conf: OptionalConfigEntry[String],
      secretName: String): Option[(String, String)] = {
    sparkConf.get(conf).map(new File(_)).map { file =>
      if (!file.isFile()) {
        throw new SparkException(s"File provided for ${conf.key} at ${file.getAbsolutePath}" +
          s" does not exist or is not a file.")
      }
      (secretName, BaseEncoding.base64().encode(Files.toByteArray(file)))
    }
  }
}
