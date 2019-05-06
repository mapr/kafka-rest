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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Synchronized wrapper for KafkaConsumer that
 * is used to fetch metadata about topics for
 * both MapR Streams and Kafka.
 *
 * <p>Note: for Kafka some APIs are unavailable
 * and en exception is thrown.
 */
class StreamsMetadataConsumer implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(StreamsMetadataConsumer.class);

  private KafkaConsumer<byte[], byte[]> metadataConsumer;
  private Lock consumerLock;

  StreamsMetadataConsumer(String bootstrapServers, String defaultStream) {
    Properties properties = new Properties();
    if (bootstrapServers != null) {
      properties.setProperty(
           ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }
    boolean defaultStreamProvided = false;
    if (!"".equals(defaultStream)) {
      properties.setProperty(
           ConsumerConfig.STREAMS_CONSUMER_DEFAULT_STREAM_CONFIG, defaultStream);
      defaultStreamProvided = true;
    }
    metadataConsumer =
          new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());

    if (!defaultStreamProvided) {
      // We need to have initialized KafkaConsumer driver right after
      // creation if default stream was not specified.
      // This forces consumer initialization in Streams mode.
      try {
        Method method = metadataConsumer.getClass()
                .getDeclaredMethod("initializeConsumer", String.class);
        method.setAccessible(true);
        method.invoke(metadataConsumer, "/:");
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        log.warn("Metadata consumer error: ", e);
      }
    }

    this.consumerLock = new ReentrantLock(true);
  }

  /**
   * Available for MapR Streams
   * @return list of topics for a specific stream
   */
  Map<String, List<PartitionInfo>> listTopics(String stream) {
    consumerLock.lock();
    try {
      return metadataConsumer.listTopics(stream);
    } finally {
      consumerLock.unlock();
    }
  }

  /**
   * @return all kafka topics in case of Kafka backend
   *         or topics in default stream in case of MapR Streams backend.
   */
  Map<String, List<PartitionInfo>> listTopics() {
    consumerLock.lock();
    try {
      return metadataConsumer.listTopics();
    } finally {
      consumerLock.unlock();
    }
  }

  List<PartitionInfo> partitionsFor(String topic) {
    consumerLock.lock();
    try {
      return metadataConsumer.partitionsFor(topic);
    } catch (UnknownTopicOrPartitionException e) {
      throw Errors.topicNotFoundException();
    } finally {
      consumerLock.unlock();
    }
  }


  @Override
  public void close() {
    metadataConsumer.wakeup();
    consumerLock.lock();
    try {
      metadataConsumer.close();
    } finally {
      consumerLock.unlock();
    }
  }
}
