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
package com.google.cloud.pso.beam.transforms.kafka;

import com.google.cloud.pso.beam.options.KafkaOptions;
import com.google.cloud.pso.beam.options.KafkaSASLSSLOptions;
import org.apache.beam.sdk.options.PipelineOptions;

public class SASLSSLConfig extends KafkaConfig {

  final String keytabId;
  final String trutstoreId;
  final String kerberosPrincipal;
  final String kerberosRealm;
  final String projectId;

  public SASLSSLConfig(
      String keytabId,
      String trutstoreId,
      String kerberosPrincipal,
      String groupId,
      Integer partitionMaxFetchSize,
      Boolean autoCommit,
      Integer defaultAPITimeout,
      Boolean isSecureAccessEnabled,
      String projectId,
      String bootstrapServers,
      String tssMaterialsRootFolder,
      String kerberosRealm) {
    super(
        groupId,
        partitionMaxFetchSize,
        autoCommit,
        defaultAPITimeout,
        isSecureAccessEnabled,
        bootstrapServers,
        tssMaterialsRootFolder);
    this.keytabId = keytabId;
    this.trutstoreId = trutstoreId;
    this.kerberosPrincipal = kerberosPrincipal;
    this.kerberosRealm = kerberosRealm;
    this.projectId = projectId;
  }

  public String getKeytabId() {
    return this.keytabId;
  }

  public String getTrutstoreId() {
    return this.trutstoreId;
  }

  public String getKerberosPrincipal() {
    return this.kerberosPrincipal;
  }

  public String getProjectId() {
    return this.projectId;
  }

  public static SASLSSLConfig fromOptions(PipelineOptions options) {
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
