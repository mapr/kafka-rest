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

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.entities.AvroConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Properties;

/**
 * Avro implementation of ConsumerState, which decodes into GenericRecords or primitive types.
 */
public class AvroConsumerState extends ConsumerState<Object, Object, JsonNode, JsonNode> {

  // Note that this could be a static variable and shared, but that causes tests to break in
  // subtle ways because it causes state to be shared across tests, but only for the consumer.
  private Deserializer<Object> deserializer = null;

  public AvroConsumerState(KafkaRestConfig config,
                           ConsumerInstanceId instanceId,
                           Properties consumerProperties,
                           ConsumerManager.ConsumerFactory consumerFactory) {
    super(config, instanceId, consumerProperties, consumerFactory);
    super.startHeartbeatThread();
  }

  private Deserializer<Object> initDeserializer() {
    if (deserializer == null) {
      Properties props = new Properties();
      props.setProperty("schema.registry.url",
          config.getSchemaRegistryUrl());
      boolean isAuthenticationEnabled =
              config.getBoolean(KafkaRestConfig.ENABLE_AUTHENTICATION_CONFIG);
      if (isAuthenticationEnabled) {
        props.setProperty(SchemaRegistryClientConfig.MAPRSASL_AUTH_CONFIG, "true");
      }
      deserializer = new KafkaAvroDeserializer();
      deserializer.configure((Map)props, true);
    }
    return deserializer;
  }

  @Override
  protected Deserializer<Object> getKeyDeserializer() {
    return initDeserializer();
  }

  @Override
  protected Deserializer<Object> getValueDeserializer() {
    return initDeserializer();
  }

  @Override
  public ConsumerRecordAndSize<JsonNode, JsonNode> convertConsumerRecord(
      ConsumerRecord<Object, Object> msg) {
    AvroConverter.JsonNodeAndSize keyNode = AvroConverter.toJson(msg.key());
    AvroConverter.JsonNodeAndSize valueNode = AvroConverter.toJson(msg.value());
    return new ConsumerRecordAndSize<>(
            new AvroConsumerRecord(msg.topic(),
                                   keyNode.json,
                                   valueNode.json,
                                   msg.partition(),
                                   msg.offset()),
            keyNode.size + valueNode.size
    );
  }
}
