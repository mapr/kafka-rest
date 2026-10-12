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

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import org.apache.kafka.clients.producer.Producer;

public class ProducerCacheImpl implements ProducerCache {

  private final KafkaRestContext context;
  private final Map<String, Producer<byte[], byte[]>> userProducers = new ConcurrentHashMap<>();

  @Inject
  private ProducerCacheImpl(KafkaRestContext context) {
    this.context = requireNonNull(context);
  }

  @Override
  public Producer<byte[], byte[]> getProducer() {
    return userProducers.computeIfAbsent(
        ImpersonationUtils.getCurrentUserName(), k -> context.getProducer());
  }

  @PreDestroy
  public void close() {
    userProducers.values().forEach(Producer::close);
  }
}
