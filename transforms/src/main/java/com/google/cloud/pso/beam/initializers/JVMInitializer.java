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
package com.google.cloud.pso.beam.initializers;

import com.google.cloud.pso.beam.options.KafkaOptions;
import com.google.cloud.pso.beam.options.KafkaSASLSSLOptions;
import com.google.cloud.pso.beam.transforms.kafka.KafkaAuth;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JVMInitializer
        implements JvmInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(JVMInitializer.class);

  @Override
  public void onStartup() {
    LOG.info("Call onStartup().");
  }

  @Override
  public void beforeProcessing(PipelineOptions options) {
    LOG.info("Call beforeProcessing().");
    if (options.as(KafkaOptions.class).isKafkaSASLSSLEnabled()) {
      LOG.info("setting kafka kerberos auth properties...");
      String krbFileLocation = options.as(KafkaOptions.class).getKeysRootFolder() + "/krb5.conf";
      KafkaAuth.setupKerberosConfigFile(krbFileLocation);
      System.setProperty("sun.net.spi.nameservice.provider.1", "dns,sun");
      System.setProperty("java.security.krb5.conf", krbFileLocation);
      if (options.as(KafkaSASLSSLOptions.class).isKerberosDebug()) {
        System.setProperty("sun.security.krb5.debug", "true");
      }
    }
  }
}
