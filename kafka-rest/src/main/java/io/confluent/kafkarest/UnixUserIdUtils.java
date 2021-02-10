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

import com.google.common.base.Suppliers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.IdMappingServiceProvider;
import org.apache.hadoop.security.ShellBasedIdMapping;
import org.apache.kafka.common.KafkaException;

import java.io.IOException;
import java.util.function.Supplier;

public class UnixUserIdUtils {
  private static final Supplier<IdMappingServiceProvider> LAZY_UNIX_ID_MAPPER
      = Suppliers.memoize(UnixUserIdUtils::createShellBasedIdMapping)::get;

  private static ShellBasedIdMapping createShellBasedIdMapping() {
    try {
      return new ShellBasedIdMapping(new Configuration());
    } catch (IOException e) {
      throw new KafkaException("Cannot initialize ShellBasedIdMapping mapper");
    }
  }

  public static IdMappingServiceProvider getUnixIdMapper() {
    return LAZY_UNIX_ID_MAPPER.get();
  }
}
