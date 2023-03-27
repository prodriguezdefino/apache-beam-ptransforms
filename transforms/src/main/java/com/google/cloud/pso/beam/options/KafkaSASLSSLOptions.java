/*
 * Copyright (C) 2023 Google Inc.
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
package com.google.cloud.pso.beam.options;

import com.google.cloud.pso.beam.transforms.kafka.SASLSSLConfig;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface KafkaSASLSSLOptions extends KafkaOptions {

  @Description("The keytab id on SecretManager")
  ValueProvider<String> getSecretManagerKeyTabId();

  void setSecretManagerKeyTabId(ValueProvider<String> value);

  @Description("The principal name used for Kerberos auth.")
  ValueProvider<String> getKerberosPrincipalName();

  void setKerberosPrincipalName(ValueProvider<String> value);

  @Description("The Kerberos realm in use.")
  ValueProvider<String> getKerberosRealm();

  void setKerberosRealm(ValueProvider<String> value);

  @Description("Enables Kerberos auth debug.")
  @Default.Boolean(false)
  Boolean isKerberosDebug();

  void setKerberosDebug(Boolean value);
  
  @Description("The project where SecretManager is storing the needed materials.")
  ValueProvider<String> getSecretManagerProjectId();

  void setSecretManagerProjectId(ValueProvider<String> value);

  @Description("The truststore id on SecretManager")
  ValueProvider<String> getSecretManagerTrustStoreId();

  void setSecretManagerTrustStoreId(ValueProvider<String> value);

  @Description("Retrieves a fully initialized Kafka config object.")
  @Default.InstanceFactory(KafkaConfigFactory.class)
  SASLSSLConfig getSASLSSLConfig();

  void setSASLSSLConfig(SASLSSLConfig value);

  class KafkaConfigFactory implements DefaultValueFactory<SASLSSLConfig> {

    @Override
    public SASLSSLConfig create(PipelineOptions options) {
      if (!options.as(KafkaOptions.class).isKafkaSASLSSLEnabled()) {
        throw new IllegalArgumentException("Set kafka secure access on true.");
      }
      var opts = options.as(KafkaSASLSSLOptions.class);
      return new SASLSSLConfig(
          opts.getSecretManagerKeyTabId().get(),
          opts.getSecretManagerTrustStoreId().get(),
          opts.getKerberosPrincipalName().get(),
          opts.getConsumerGroupId().get(),
          opts.getPartitionMaxFetchSize(),
          opts.isKafkaAutocommitEnabled(),
          opts.getDefaultApiTimeoutMs(),
          opts.isKafkaSASLSSLEnabled(),
          opts.getSecretManagerProjectId().get(),
          opts.getBootstrapServers().get(),
          opts.getKeysRootFolder(),
          opts.getKerberosRealm().get());
    }
  }
}
