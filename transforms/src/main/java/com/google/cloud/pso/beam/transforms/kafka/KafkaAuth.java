/*
 * Copyright (C) 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.pso.beam.transforms.kafka;

import com.google.cloud.pso.beam.secretmanager.SecretManagerHandler;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAuth {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAuth.class);
  private static final String KERBEROS_CONFIG_FILE_TEMPLATE_LOCATION = "krb.conf";

  static String getKeytabLocation(SASLSSLConfig saslSSLConfig) {
    return saslSSLConfig.keysRootFolder + "/keytabs/" + saslSSLConfig.kerberosPrincipal + ".keytab";
  }

  static String getTruststoreLocation(SASLSSLConfig saslSSLConfig) {
    return saslSSLConfig.keysRootFolder + "/truststore/ts.jks";
  }

  static String getJAASConfigTxt(SASLSSLConfig saslSSLConfig) {
    return "com.sun.security.auth.module.Krb5LoginModule required principal=\""
            + saslSSLConfig.kerberosPrincipal + "@" + saslSSLConfig.kerberosRealm
            + "\" debug=true useKeyTab=true storeKey=true keyTab=\""
            + KafkaAuth.getKeytabLocation(saslSSLConfig) + "\" doNotPrompt=true;";
  }

  public static void setupKeyTabOnWorker(SASLSSLConfig saslSSLConfig) {
    var location = KafkaAuth.getKeytabLocation(saslSSLConfig);
    LOG.debug("writing file with decoded key {} on location {}", saslSSLConfig.keytabId, location);
    KafkaAuth.setupMaterialOnWorker(location, saslSSLConfig.projectId, saslSSLConfig.keytabId);
  }

  public static void setupTrustStoreOnWorker(SASLSSLConfig saslSSLConfig) {
    var location = KafkaAuth.getTruststoreLocation(saslSSLConfig);
    LOG.debug("writing file with decoded key {} on location {}", saslSSLConfig.trutstoreId, location);
    KafkaAuth.setupMaterialOnWorker(location, saslSSLConfig.projectId, saslSSLConfig.trutstoreId);
  }

  public static void setupKerberosConfigFile(String location) {
    try {
      var configContent = Resources.toString(Resources.getResource(
              KERBEROS_CONFIG_FILE_TEMPLATE_LOCATION), Charset.defaultCharset());
      LOG.debug("writing keberos config file {} \n on location {}.", configContent, location);
      KafkaAuth.setupFileOnWorkerFS(location, () -> configContent.getBytes());
    } catch (IOException ex) {
      LOG.error("Error occurred while trying to setup kerberos config file.", ex);
    }
  }

  static void setupMaterialOnWorker(String location, String materialGCPProjectId, String materialKey) {
    KafkaAuth.setupFileOnWorkerFS(location,
            () -> KafkaAuth.retrieveSecretValue(materialGCPProjectId, materialKey));
  }

  static synchronized void setupFileOnWorkerFS(String location, Supplier<byte[]> fileContent) {
    try {
      if (Files.exists(Paths.get(location))) {
        return;
      }
      if (Files.notExists(Paths.get(location).getParent())) {
        KafkaAuth.createRootFolder(Paths.get(location).getParent().toString());
      }
      Files.write(Paths.get(location), fileContent.get());
    } catch (IOException ex) {
      var msg = "Exception occurred while trying to write material on disk: " + location;
      LOG.error(msg, (Throwable) ex);
      throw new RuntimeException(msg, (Throwable) ex);
    }
  }

  static void createRootFolder(String path) {
    try {
      Path materialsDir = Paths.get(path);
      if (!Files.exists(materialsDir)) {
        Files.createDirectories(materialsDir);
      }
    } catch (IOException ex) {
      var msg = "Exception occurred while trying to create materials folder: " + path;
      LOG.error(msg, (Throwable) ex);
      throw new RuntimeException(msg, (Throwable) ex);
    }
  }

  public static Map<String, Object> populateAuthConfigProperties(
          Map<String, Object> config, SASLSSLConfig saslSSLConfig) {
    config.put("security.protocol", SecurityProtocol.SASL_SSL.toString());
    config.put("sasl.mechanism", "GSSAPI");
    config.put("sasl.kerberos.service.name", "kafka");
    config.put("sasl.kerberos.server.name", "kafka");
    config.put("sasl.jaas.config", KafkaAuth.getJAASConfigTxt(saslSSLConfig));
    config.put("ssl.truststore.location", KafkaAuth.getTruststoreLocation(saslSSLConfig));
    return config;
  }

  public static String ipAddressFormForBootstrapServers(String bootstrapServers) {
    return Arrays.stream(bootstrapServers.split(","))
            .map(s -> {
              String[] server = s.split(":");
              try {
                return InetAddress.getByName((String) server[0]).getHostAddress() + ":" + server[1];
              } catch (UnknownHostException ex) {
                throw new RuntimeException("Problems while resolving hostnames.", (Throwable) ex);
              }
            }).collect(Collectors.joining((CharSequence) ","));
  }

  private static byte[] retrieveSecretValue(String projectId, String secretId) {
    var response = SecretManagerHandler.getSecretValue(projectId, secretId);
    return response.getPayload().getData().toByteArray();
  }
}
