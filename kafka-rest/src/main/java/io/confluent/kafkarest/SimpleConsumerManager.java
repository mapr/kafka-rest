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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafkarest.entities.AvroConsumerRecord;
import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.JsonConsumerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestServerErrorException;

import org.apache.kafka.common.errors.SerializationException;

public class SimpleConsumerManager {

  private static final Logger log = LoggerFactory.getLogger(SimpleConsumerManager.class);

  private final int maxPoolSize;
  private final int maxPollTime;
  private final int poolInstanceAvailabilityTimeoutMs;
  private final Time time;

  private final KafkaStreamsMetadataObserver mdObserver;
  private final SimpleConsumerFactory simpleConsumerFactory;

  // stores pull of KafkaConsumer objects
  private final SimpleConsumerPool simpleConsumersPool;

  private final SimpleConsumerRecordsCache cache;

  private final Deserializer<Object> avroDeserializer;
  private final ObjectMapper objectMapper;

  public SimpleConsumerManager(
      final KafkaRestConfig config,
      final KafkaStreamsMetadataObserver mdObserver,
      final SimpleConsumerFactory simpleConsumerFactory
  ) {

    this.mdObserver = mdObserver;
    this.simpleConsumerFactory = simpleConsumerFactory;

    maxPoolSize = config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_MAX_POOL_SIZE_CONFIG);
    maxPollTime = config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_MAX_POLL_TIME_CONFIG);
    poolInstanceAvailabilityTimeoutMs =
            config.getInt(KafkaRestConfig.SIMPLE_CONSUMER_POOL_TIMEOUT_MS_CONFIG);
    time = config.getTime();

    simpleConsumersPool =
      new SimpleConsumerPool(maxPoolSize, poolInstanceAvailabilityTimeoutMs,
              time, simpleConsumerFactory, mdObserver);
    cache =
      new SimpleConsumerRecordsCache(config);

    // Load decoders
    Properties props = new Properties();
    props.setProperty("schema.registry.url",
            config.getSchemaRegistryUrl());
    boolean isAuthenticationEnabled =
            config.getBoolean(KafkaRestConfig.ENABLE_AUTHENTICATION_CONFIG);
    if (isAuthenticationEnabled) {
      props.setProperty(SchemaRegistryClientConfig.MAPRSASL_AUTH_CONFIG, "true");
    }
    avroDeserializer = new KafkaAvroDeserializer();
    avroDeserializer.configure((Map)props, true);
    objectMapper = new ObjectMapper();

  }

  public void consume(final String topicName,
                      final int partitionId,
                      long offset,
                      long count,
                      final EmbeddedFormat embeddedFormat,
                      final ConsumerManager.ReadCallback callback) {

    List<ConsumerRecord> result = new ArrayList<>();
    RestException exception = null;

    if (!mdObserver.topicExists(topicName)) {
      callback.onCompletion(null, Errors.topicNotFoundException());
      return;
    }
    if (!mdObserver.partitionExists(topicName, partitionId)) {
      callback.onCompletion(null, Errors.partitionNotFoundException());
      return;
    }
    try (TPConsumerState consumer = simpleConsumersPool.get(topicName, partitionId)) {
      Consumer<byte[], byte[]> assignedConsumer = consumer.consumer();
      long pollTimeout = maxPollTime;
      assignedConsumer.seek(new TopicPartition(topicName, partitionId), offset);
      long endTime = time.milliseconds() + pollTimeout;
      Iterator<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> it = null;
      boolean enough = false;
      while (!enough && (pollTimeout = endTime - time.milliseconds()) > 0) {
        if (it == null || !it.hasNext()) {
          ConsumerRecords<byte[], byte[]> records = assignedConsumer.poll(Math.max(0, pollTimeout));
          if (records.isEmpty()) {
            continue;
          }
          it = records.iterator();
        }
        while (it.hasNext()) {
          org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record = it.next();
          result.add(createConsumerRecord(record, topicName, partitionId, embeddedFormat));
          if (result.size() == count) {
            enough = true;
            break;
          }
        }
      }
    } catch (Throwable e) {
      if (e instanceof RestException) {
        exception = (RestException) e;
      } else {
        exception = Errors.kafkaErrorException(e);
        log.warn("Internal error", e);
      }
    }
    callback.onCompletion(result, exception);
  }

  private BinaryConsumerRecord createBinaryConsumerRecord(
      final org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> consumerRecord,
      final String topicName,
      final int partitionId) {

    // KafkaConsumer instances are created with ByteArrayDeserializer so
    // there is no reason to deserialize record again.
    return new BinaryConsumerRecord(topicName, consumerRecord.key(), consumerRecord.value(),
         partitionId, consumerRecord.offset());
  }

  private AvroConsumerRecord createAvroConsumerRecord(
      final org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> consumerRecord,
      final String topicName,
      final int partitionId) {

    return new AvroConsumerRecord(
        topicName,
        AvroConverter.toJson(avroDeserializer.deserialize(topicName, consumerRecord.key())).json,
        AvroConverter.toJson(avroDeserializer.deserialize(topicName, consumerRecord.value())).json,
        partitionId,
        consumerRecord.offset());
  }


  private JsonConsumerRecord createJsonConsumerRecord(
      final org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> consumerRecord,
      final String topicName,
      final int partitionId) {

    return new JsonConsumerRecord(
        topicName,
        deserializeJson(consumerRecord.key()),
        deserializeJson(consumerRecord.value()),
        partitionId, consumerRecord.offset());
  }

  private Object deserializeJson(byte[] data) {
    try {
      return data == null ? null : objectMapper.readValue(data, Object.class);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  private ConsumerRecord createConsumerRecord(
      final org.apache.kafka.clients.consumer.ConsumerRecord consumerRecord,
      final String topicName,
      final int partitionId,
      final EmbeddedFormat embeddedFormat) {

    switch (embeddedFormat) {
      case BINARY:
        return createBinaryConsumerRecord(consumerRecord, topicName, partitionId);

      case AVRO:
        return createAvroConsumerRecord(consumerRecord, topicName, partitionId);

      case JSON:
        return createJsonConsumerRecord(consumerRecord, topicName, partitionId);

      default:
        throw new RestServerErrorException("Invalid embedded format for new consumer.",
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  public void shutdown() {
    simpleConsumersPool.shutdown();
  }

}
