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
package com.google.cloud.pso.beam.secretmanager;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecretManagerHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SecretManagerHandler.class);

  public static AccessSecretVersionResponse getSecretValue(String projectId, String secretId) {
    var encodedKey = Base64.getEncoder().encodeToString(secretId.getBytes());
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      LOG.info("retrieving encoded key secret {} from projectid {}", encodedKey, projectId);
      var secretVersionName
              = SecretVersionName.of(projectId, encodedKey, "latest");
      return client.accessSecretVersion(secretVersionName);
    } catch (Exception ex) {
      var msg = "Error while interacting with SecretManager client, key: " + encodedKey;
      LOG.error(msg, ex);
      throw new RuntimeException(msg, ex);
    }
  }

}
