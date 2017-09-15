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

import kafka.utils.ZkUtils;

/**
 * Shared, global state for the REST proxy server, including configuration and connection pools.
 */
public class Context {

  private final KafkaRestConfig config;
  private  KafkaStreamsMetadataObserver metadataObserver;
  private final ProducerPool producerPool;
  private final ConsumerManager consumerManager;
  private final SimpleConsumerManager simpleConsumerManager;
  private final ZkUtils zkUtils;
  private final boolean isStreams;
  private final boolean isImpersonationEnabled;
  public Context(KafkaRestConfig config, KafkaStreamsMetadataObserver metadataObserver,
                 ProducerPool producerPool, ConsumerManager consumerManager,
                 SimpleConsumerManager simpleConsumerManager, ZkUtils zkUtils, boolean isStreams,
                 boolean isImpersonationEnabled) {
    this.config = config;
    this.metadataObserver = metadataObserver;
    this.producerPool = producerPool;
    this.consumerManager = consumerManager;
    this.simpleConsumerManager = simpleConsumerManager;
    this.zkUtils = zkUtils;
    this.isStreams = isStreams;
    this.isImpersonationEnabled = isImpersonationEnabled;
  }

  public KafkaRestConfig getConfig() {
    return config;
  }

  public KafkaStreamsMetadataObserver getMetadataObserver() {
      if (isImpersonationEnabled) {
          return new KafkaStreamsMetadataObserver(config, zkUtils, isStreams, true);
      } else {
          return metadataObserver;
      }
  }

  public ProducerPool getProducerPool() {
      if(isImpersonationEnabled) {
          return new ProducerPool(config, zkUtils);
      } else {
          return producerPool;
      }
  }

  public ConsumerManager getConsumerManager() {
    return consumerManager;
  }

  public SimpleConsumerManager getSimpleConsumerManager() {
    if(isImpersonationEnabled) {
        return new SimpleConsumerManager(config, getMetadataObserver(), new SimpleConsumerFactory(config));
    } else {
        return simpleConsumerManager;
    }
  }

  public ZkUtils getZkUtils() {
    return zkUtils;
  }

  public boolean isStreams() {
    return isStreams;
  }
  
  public boolean isImpersonationEnabled() {
    return isImpersonationEnabled;
  }
}
