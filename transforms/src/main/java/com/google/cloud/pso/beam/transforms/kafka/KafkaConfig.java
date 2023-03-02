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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(value = {@JsonSubTypes.Type(value = SASLSSLConfig.class, name = "saslsslconfig")})
public class KafkaConfig implements Serializable {

  final String groupId;
  final Integer partitionMaxFetchSize;
  final Boolean autoCommit;
  final Integer defaultAPITimeout;
  final Boolean isSecureAccessEnabled;
  final String projectId;
  final String bootstrapServers;
  final String keysRootFolder;

  public KafkaConfig(
      String groupId,
      Integer partitionMaxFetchSize,
      Boolean autoCommit,
      Integer defaultAPITimeout,
      Boolean isSecureAccessEnabled,
      String projectId,
      String bootstrapServers,
      String keysRootFolder) {
    this.groupId = groupId;
    this.partitionMaxFetchSize = partitionMaxFetchSize;
    this.autoCommit = autoCommit;
    this.defaultAPITimeout = defaultAPITimeout;
    this.isSecureAccessEnabled = isSecureAccessEnabled;
    this.projectId = projectId;
    this.bootstrapServers = bootstrapServers;
    this.keysRootFolder = keysRootFolder;
  }

  @SuppressWarnings("unchecked")
  public <T extends KafkaConfig> T as(Class<T> clazz) {
    return (T) this;
  }

  public String getGroupId() {
    return this.groupId;
  }

  public Integer getPartitionMaxFetchSize() {
    return this.partitionMaxFetchSize;
  }

  public Boolean getAutoCommit() {
    return this.autoCommit;
  }

  public Integer getDefaultAPITimeout() {
    return this.defaultAPITimeout;
  }

  public Boolean getIsSecureAccessEnabled() {
    return this.isSecureAccessEnabled;
  }

  public String getProjectId() {
    return this.projectId;
  }

  public String getBootstrapServers() {
    return this.bootstrapServers;
  }

  public String getKeysRootFolder() {
    return this.keysRootFolder;
  }
}
