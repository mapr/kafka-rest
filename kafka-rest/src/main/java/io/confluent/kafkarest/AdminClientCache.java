/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest;

import org.apache.hadoop.security.IdMappingServiceProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class AdminClientCache {

  private final Map<Integer, AdminClient> adminClients = new ConcurrentHashMap<>();
  private final IdMappingServiceProvider uidMapper;

  public AdminClientCache(IdMappingServiceProvider uidMapper) {
    this.uidMapper = uidMapper;
  }

  public AdminClient get(Properties config) {
    try {
      final String userName = UserGroupInformation.getCurrentUser().getUserName();
      final int userUid = uidMapper.getUid(userName);
      return adminClients.computeIfAbsent(userUid, uid -> AdminClient.create(config));
    } catch (Exception e) {
      throw Errors.serverLoginException(e);
    }
  }

  public void close() {
    for (AdminClient adminClient : adminClients.values()) {
      adminClient.close();
    }
  }
}
