/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The SizeLimitedSimpleConsumerPool keeps a pool of SimpleConsumers
 * and can increase the pool within a specified limit
 */
public class SimpleConsumerPool {
  private static final Logger log = LoggerFactory.getLogger(SimpleConsumerPool.class);

  // maxPoolSize = 0 means unlimited
  private final int maxPoolSize;
  // poolInstanceAvailabilityTimeoutMs = 0 means there is no timeout
  private final int poolInstanceAvailabilityTimeoutMs;
  private final Time time;

  private final SimpleConsumerFactory simpleConsumerFactory;
  private final Map<String, TPConsumerState> simpleConsumers;

  private final Queue<String> availableKafkaConsumers;
  private final Queue<String> availableStreamsConsumers;
  private final KafkaStreamsMetadataObserver metadataObserver;

  private final Lock poolLock;

  public SimpleConsumerPool(int maxPoolSize, int poolInstanceAvailabilityTimeoutMs,
                            Time time, SimpleConsumerFactory simpleConsumerFactory, KafkaStreamsMetadataObserver metadataObserver) {
    this.maxPoolSize = maxPoolSize;
    this.poolInstanceAvailabilityTimeoutMs = poolInstanceAvailabilityTimeoutMs;
    this.time = time;
    this.simpleConsumerFactory = simpleConsumerFactory;
    this.metadataObserver = metadataObserver;
    this.poolLock = new ReentrantLock(true);
    simpleConsumers = new HashMap<String, TPConsumerState>();
    availableKafkaConsumers = new LinkedList<String>();
    availableStreamsConsumers = new LinkedList<String>();
  }

  /**
   * @return assigned Consumer that is ready to be used for polling records
   */
  public synchronized TPConsumerState get(String topic, int partition) {

    final long expiration = time.milliseconds() + poolInstanceAvailabilityTimeoutMs;

    while (true) {
      // is the request for a given topic passed to Streams
      boolean requestToStreams = metadataObserver.requestToStreams(topic);

      String consumerId = null;
      if (requestToStreams) {
        // If there is a streams SimpleConsumer available
        if (availableStreamsConsumers.size() > 0) {
          consumerId = availableStreamsConsumers.remove();
        }
      } else {
        // If there is a kafka SimpleConsumer available
        if (availableKafkaConsumers.size() > 0) {
          consumerId = availableKafkaConsumers.remove();
        }
      }

      if (consumerId != null) {
        TPConsumerState consumerState = simpleConsumers.get(consumerId);
        consumerState.consumer().assign(Collections
          .singletonList(new TopicPartition(topic, partition)));

        return consumerState;
      }

      // If not consumer is available, but we can instantiate a new one
      if (simpleConsumers.size() < maxPoolSize || maxPoolSize == 0) {
        return createAndAssign(topic, partition, requestToStreams);
      }

      // If no consumer is available and we reached the limit
      try {
        // The behavior of wait when poolInstanceAvailabilityTimeoutMs=0 is consistent as it won't timeout
        wait(poolInstanceAvailabilityTimeoutMs);
      } catch (InterruptedException e) {
        log.warn("A thread requesting a SimpleConsumer has been interrupted while waiting", e);
      }

      // In some cases ("spurious wakeup", see wait() doc), the thread will resume before the timeout
      // We have to guard against that and throw only if the timeout has expired for real
      if (time.milliseconds() > expiration && poolInstanceAvailabilityTimeoutMs != 0) {
        boolean removed = false;
        try {
          // Since we cannot reuse a consumer for different backends once it was assigned
          // we will remove available consumer from another backend (if such exists) and
          // create new one for requested backend.
          if (requestToStreams) {
            if (availableKafkaConsumers.size() > 0) {
              TPConsumerState removedConsumer = simpleConsumers.remove(availableKafkaConsumers.remove());
              removedConsumer.consumer().close();
              removed = true;
            }
          } else {
            if (availableStreamsConsumers.size() > 0) {
              TPConsumerState removedConsumer = simpleConsumers.remove(availableStreamsConsumers.remove());
              removedConsumer.consumer().close();
              removed = true;
            }
          }
        } catch (Exception e) {
          log.warn("Exception while closing consumer", e);
        }

        if (removed) {
          return createAndAssign(topic, partition, requestToStreams);
        } else {
          throw Errors.simpleConsumerPoolTimeoutException();
        }
      }
    }
  }

  private synchronized TPConsumerState createAndAssign(String topic, int partition, boolean isStreamsConsumer) {
    final SimpleConsumerFactory.ConsumerProvider simpleConsumer = simpleConsumerFactory.createConsumer();

    // assign consumer to TopicPartition
    // depending on topic string and default streams presence
    // KafkaConsumer is marked as Streams or Kafka consumer.
    // The variable isStreamsConsumer must be set to the correct
    // value that corresponds to topic
    simpleConsumer.consumer().assign(Collections
      .singletonList(new TopicPartition(topic, partition)));

    TPConsumerState consumerState =
      new TPConsumerState(simpleConsumer.consumer(), isStreamsConsumer, this, simpleConsumer.clientId());
    simpleConsumers.put(simpleConsumer.clientId(), consumerState);
    return consumerState;
  }


  synchronized public void release(TPConsumerState tpConsumerState) {
    log.debug("Releasing into the pool SimpleConsumer with id " + tpConsumerState.clientId());
    if (tpConsumerState.isStreams()) {
      availableStreamsConsumers.add(tpConsumerState.clientId());
    } else {
      availableKafkaConsumers.add(tpConsumerState.clientId());
    }
    notify();
  }

  public void shutdown() {
    for (TPConsumerState consumer : simpleConsumers.values()) {
      consumer.consumer().wakeup();
      consumer.consumer().close();
    }
  }

  public int size() {
    return simpleConsumers.size();
  }
}
