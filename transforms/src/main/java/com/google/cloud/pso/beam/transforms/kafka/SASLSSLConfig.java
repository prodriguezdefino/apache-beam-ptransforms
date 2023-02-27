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

public class SASLSSLConfig extends KafkaConfig {

  final String keytabId;
  final String trutstoreId;
  final String kerberosPrincipal;
  final String kerberosRealm;

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
        projectId,
        bootstrapServers,
        tssMaterialsRootFolder);
    this.keytabId = keytabId;
    this.trutstoreId = trutstoreId;
    this.kerberosPrincipal = kerberosPrincipal;
    this.kerberosRealm = kerberosRealm;
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
}
