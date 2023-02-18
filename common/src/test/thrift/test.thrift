/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 
namespace java com.google.cloud.pso.beam.generator.thrift

struct User {
  1: required string location
  2: required i64    startup
  3: optional string description
  4: required string uuid
}

struct Topic {
  1: required string id
  2: required i64    value
  3: required string name
}

struct Message {
   1: required User   user
   2: required Topic  topic
   3: required string message
}

