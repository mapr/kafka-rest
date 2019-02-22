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

import io.confluent.kafkarest.entities.BinaryConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Properties;

/**
 * Binary implementation of ConsumerState that does no decoding, returning the raw bytes directly.
 */
public class BinaryConsumerState extends ConsumerState<byte[], byte[], byte[], byte[]> {

  private static final Deserializer<byte[]> deserializer = new ByteArrayDeserializer();

  public BinaryConsumerState(KafkaRestConfig config,
                             ConsumerInstanceId instanceId,
                             Properties consumerProperties,
                             ConsumerManager.ConsumerFactory consumerFactory) {
    super(config, instanceId, consumerProperties, consumerFactory);
  }

  @Override
  protected Deserializer<byte[]> getKeyDeserializer() {
    return deserializer;
  }

  @Override
  protected Deserializer<byte[]> getValueDeserializer() {
    return deserializer;
  }

  @Override
  public ConsumerRecordAndSize<byte[], byte[]> convertConsumerRecord(
      ConsumerRecord<byte[], byte[]> msg) {
    long approxSize = (msg.key() != null ? msg.key().length : 0)
        + (msg.value() != null ? msg.value().length : 0);
    return new ConsumerRecordAndSize<>(
        new BinaryConsumerRecord(msg.topic(),
                                 msg.key(),
                                 msg.value(),
                                 msg.partition(),
                                 msg.offset()),
        approxSize);
  }

}
