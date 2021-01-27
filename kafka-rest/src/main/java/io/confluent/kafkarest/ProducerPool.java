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
import org.apache.hadoop.security.UserGroupInformation;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafkarest.converters.AvroConverter;
import io.confluent.kafkarest.converters.JsonSchemaConverter;
import io.confluent.kafkarest.converters.ProtobufConverter;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import  io.confluent.rest.exceptions.RestServerErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import javax.ws.rs.core.Response;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
/**
 * Shared pool of Kafka producers used to send messages. The pool manages batched sends, tracking
 * all required acks for a batch and managing timeouts. Currently this pool only contains one
 * producer per serialization format (e.g. byte[], Avro).
 */
public class ProducerPool {

  private static final Logger log = LoggerFactory.getLogger(ProducerPool.class);
  private Map<EmbeddedFormat, RestProducer> producers =
      new HashMap<EmbeddedFormat, RestProducer>();
  private SimpleProducerCache producerCache;
  private boolean defaultStreamSet;
  private boolean isImpersonationEnabled;
  private Map<String, Object> standardProps;
  private Map<String, Object> schemaProps;

  public ProducerPool(KafkaRestConfig appConfig) {
    this(appConfig, null);
  }

  public ProducerPool(
      KafkaRestConfig appConfig,
      Properties producerConfigOverrides
  ) {
    this(appConfig, RestConfigUtils.bootstrapBrokers(appConfig), producerConfigOverrides);
  }

  public ProducerPool(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
    this.defaultStreamSet = appConfig.isDefaultStreamSet();
    this.isImpersonationEnabled = appConfig.isImpersonationEnabled();

    standardProps = buildStandardConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
    schemaProps = buildSchemaConfig(appConfig, bootstrapBrokers, producerConfigOverrides);

    if (!isImpersonationEnabled) {
      producers.put(EmbeddedFormat.BINARY, buildBinaryProducer(standardProps));
      producers.put(EmbeddedFormat.JSON, buildJsonProducer(standardProps));
      producers.put(EmbeddedFormat.AVRO, buildAvroProducer(schemaProps));
      producers.put(EmbeddedFormat.JSONSCHEMA, buildJsonSchemaProducer(schemaProps));
      producers.put(EmbeddedFormat.PROTOBUF, buildProtobufProducer(schemaProps));
    } else {
      producerCache = new SimpleProducerCache(appConfig);
    }
  }

  private Map<String, Object> buildStandardConfig(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
    Map<String, Object> props = new HashMap<String, Object>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);

    Properties producerProps = (Properties) appConfig.getProducerProperties();
    // configure default stream
    String defaultStream = appConfig.getString(KafkaRestConfig.STREAMS_DEFAULT_STREAM_CONFIG);
    if (!"".equals(defaultStream)) {
      props.put(ProducerConfig.STREAMS_PRODUCER_DEFAULT_STREAM_CONFIG, defaultStream);
    }
    int streamBuffer = appConfig.getInt(KafkaRestConfig.STREAM_BUFFER_MAX_TIME_CONFIG);
    props.put(ProducerConfig.STREAMS_BUFFER_TIME_CONFIG, streamBuffer);

    return buildConfig(props, producerProps, producerConfigOverrides);
  }

  private NoSchemaRestProducer<byte[], byte[]> buildBinaryProducer(
      Map<String, Object>
          binaryProps
  ) {
    return buildNoSchemaProducer(binaryProps, new ByteArraySerializer(), new ByteArraySerializer());
  }

  private NoSchemaRestProducer<Object, Object> buildJsonProducer(Map<String, Object> jsonProps) {
    return buildNoSchemaProducer(jsonProps, new KafkaJsonSerializer(), new KafkaJsonSerializer());
  }

  private <K, V> NoSchemaRestProducer<K, V> buildNoSchemaProducer(
      Map<String, Object> props,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer
  ) {
    keySerializer.configure(props, true);
    valueSerializer.configure(props, false);
    KafkaProducer<K, V> producer =
        new KafkaProducer<K, V>(props, keySerializer, valueSerializer);
    return new NoSchemaRestProducer<K, V>(producer);
  }

  private Map<String, Object> buildSchemaConfig(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
    Map<String, Object> schemaDefaults = new HashMap<String, Object>();
    schemaDefaults.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    schemaDefaults.put(
        "schema.registry.url",
        appConfig.getSchemaRegistryUrl()
    );
    // configure default stream
    String defaultStream = appConfig.getString(KafkaRestConfig.STREAMS_DEFAULT_STREAM_CONFIG);
    if (!"".equals(defaultStream)) {
      schemaDefaults.put(ProducerConfig.STREAMS_PRODUCER_DEFAULT_STREAM_CONFIG, defaultStream);
    }
    boolean isAuthenticationEnabled =
            appConfig.getBoolean(KafkaRestConfig.ENABLE_AUTHENTICATION_CONFIG);
    if (isAuthenticationEnabled) {
      schemaDefaults.put(SchemaRegistryClientConfig.MAPRSASL_AUTH_CONFIG, "true");
    }
    int streamBuffer = appConfig.getInt(KafkaRestConfig.STREAM_BUFFER_MAX_TIME_CONFIG);
    schemaDefaults.put(ProducerConfig.STREAMS_BUFFER_TIME_CONFIG, streamBuffer);

    Properties producerProps = (Properties) appConfig.getProducerProperties();
    return buildConfig(schemaDefaults, producerProps, producerConfigOverrides);
  }

  private SchemaRestProducer buildAvroProducer(Map<String, Object> props) {
    final KafkaAvroSerializer keySerializer = new KafkaAvroSerializer();
    keySerializer.configure(props, true);
    final KafkaAvroSerializer valueSerializer = new KafkaAvroSerializer();
    valueSerializer.configure(props, false);
    KafkaProducer<Object, Object> producer
        = new KafkaProducer<Object, Object>(props, keySerializer, valueSerializer);
    return new SchemaRestProducer(producer, keySerializer, valueSerializer,
        new AvroSchemaProvider(), new AvroConverter());
  }

  private SchemaRestProducer buildJsonSchemaProducer(Map<String, Object> props) {
    final KafkaJsonSchemaSerializer keySerializer = new KafkaJsonSchemaSerializer();
    keySerializer.configure(props, true);
    final KafkaJsonSchemaSerializer valueSerializer = new KafkaJsonSchemaSerializer();
    valueSerializer.configure(props, false);
    KafkaProducer<Object, Object> producer
        = new KafkaProducer<Object, Object>(props, keySerializer, valueSerializer);
    return new SchemaRestProducer(producer, keySerializer, valueSerializer,
        new JsonSchemaProvider(), new JsonSchemaConverter());
  }

  private SchemaRestProducer buildProtobufProducer(Map<String, Object> props) {
    final KafkaProtobufSerializer keySerializer = new KafkaProtobufSerializer();
    keySerializer.configure(props, true);
    final KafkaProtobufSerializer valueSerializer = new KafkaProtobufSerializer();
    valueSerializer.configure(props, false);
    KafkaProducer<Object, Object> producer
        = new KafkaProducer<Object, Object>(props, keySerializer, valueSerializer);
    return new SchemaRestProducer(producer, keySerializer, valueSerializer,
        new ProtobufSchemaProvider(), new ProtobufConverter());
  }

  private Map<String, Object> buildConfig(
      Map<String, Object> defaults,
      Properties userProps,
      Properties overrides
  ) {
    // Note careful ordering: built-in values we look up automatically first, then configs
    // specified by user with initial KafkaRestConfig, and finally explicit overrides passed to
    // this method (only used for tests)
    Map<String, Object> config = new HashMap<String, Object>(defaults);
    for (String propName : userProps.stringPropertyNames()) {
      config.put(propName, userProps.getProperty(propName));
    }
    if (overrides != null) {
      for (String propName : overrides.stringPropertyNames()) {
        config.put(propName, overrides.getProperty(propName));
      }
    }
    return config;
  }

  public <K, V> void produce(
      String topic,
      Integer partition,
      EmbeddedFormat recordFormat,
      ProduceRequest<K, V> produceRequest,
      ProduceRequestCallback callback
  ) {
    ProduceTask task =
        new ProduceTask(
            produceRequest,
            produceRequest.getRecords().size(),
            callback);
    log.trace("Starting produce task " + task.toString());
    @SuppressWarnings("unchecked")
    RestProducer<K, V> restProducer;
    if (!defaultStreamSet && !topic.contains(":")) {
      throw Errors.topicNotFoundException();
    }
    //we enclose it only for streams producer
    //because there can be exception due to permissions
    if (isImpersonationEnabled) {
      String userName = null;
      try {
        userName = UserGroupInformation.getCurrentUser().getUserName();
      } catch (IOException e) {
        // Never happens because authentication is required for MapR impersonation
      }
      switch (recordFormat) {
        case BINARY:
          restProducer = (RestProducer<K, V>) producerCache
                  .getBinaryProducer(userName);
          break;
        case JSON:
          restProducer = (RestProducer<K, V>) producerCache
                  .getJsonProducer(userName);
          break;
        case AVRO:
          restProducer = (RestProducer<K, V>) producerCache
                  .getAvroProducer(userName);
          break;
        case JSONSCHEMA:
          restProducer = (RestProducer<K, V>) producerCache
                  .getJsonSchemaProducer(userName);
          break;
        case PROTOBUF:
          restProducer = (RestProducer<K, V>) producerCache
                  .getProtobufProducer(userName);
          break;
        default:
          throw new RestServerErrorException(
              "Invalid embedded format for new producer.",
              Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()
          );
      }
    } else {
      restProducer = (RestProducer<K, V>) producers.get(recordFormat);
    }

    restProducer.produce(
        task,
        topic,
        partition,
        produceRequest.getRecords());
  }

  public void shutdown() {
    for (RestProducer restProducer : producers.values()) {
      restProducer.close();
    }
    if (producerCache != null) {
      producerCache.shutdown();
    }
  }

  public interface ProduceRequestCallback {

    /**
     * Invoked when all messages have either been recorded or received an error
     *
     * @param results list of responses, in the same order as the request. Each entry can be either
     *                a RecordAndMetadata for successful responses or an exception
     */
    public void onCompletion(
        Integer keySchemaId,
        Integer valueSchemaId,
        List<RecordMetadataOrException> results
    );
  }

  class SimpleProducerCache {
    private final int maxCachesNum;

    /**
     * Stores insertion order of producer caches.
     */
    private Queue<String> binaryOldestCache;
    private Queue<String> jsonOldestCache;
    private Queue<String> avroOldestCache;
    private Queue<String> jsonSchemaOldestCache;
    private Queue<String> protobufOldestCache;

    private ConcurrentMap<String, RestProducer> binaryHighLevelCache;
    private ConcurrentMap<String, RestProducer> jsonHighLevelCache;
    private ConcurrentMap<String, RestProducer> avroHighLevelCache;
    private ConcurrentMap<String, RestProducer> jsonSchemaHighLevelCache;
    private ConcurrentMap<String, RestProducer> protobufHighLevelCache;

    SimpleProducerCache(final KafkaRestConfig config) {
      this.maxCachesNum = config.getInt(KafkaRestConfig.PRODUCERS_MAX_CACHES_NUM_CONFIG);

      this.binaryHighLevelCache = new ConcurrentHashMap<>(maxCachesNum);
      this.jsonHighLevelCache = new ConcurrentHashMap<>(maxCachesNum);
      this.avroHighLevelCache = new ConcurrentHashMap<>(maxCachesNum);
      this.jsonSchemaHighLevelCache = new ConcurrentHashMap<>(maxCachesNum);
      this.protobufHighLevelCache = new ConcurrentHashMap<>(maxCachesNum);

      this.binaryOldestCache = new ConcurrentLinkedQueue<>();
      this.jsonOldestCache = new ConcurrentLinkedQueue<>();
      this.avroOldestCache = new ConcurrentLinkedQueue<>();
      this.jsonSchemaOldestCache = new ConcurrentLinkedQueue<>();
      this.protobufOldestCache = new ConcurrentLinkedQueue<>();
    }

    RestProducer getBinaryProducer(final String userName) {
      RestProducer cache;
      if (maxCachesNum > 0) {
        cache = binaryHighLevelCache.get(userName);


        if (cache == null) {
          if (binaryHighLevelCache.size() >= maxCachesNum) {
            // remove eldest element from the cache
            String eldest = binaryOldestCache.poll();
            binaryHighLevelCache.remove(eldest).close();
          }

          // add new entry
          cache = buildBinaryProducer(standardProps);
          binaryHighLevelCache.put(userName, cache);
          binaryOldestCache.add(userName);
        }
      } else {
        // caching is disabled. Create producer for each request.
        cache = buildBinaryProducer(standardProps);
      }
      return cache;
    }

    RestProducer getJsonProducer(final String userName) {
      RestProducer cache;
      if (maxCachesNum > 0) {
        cache = jsonHighLevelCache.get(userName);

        if (cache == null) {
          if (jsonHighLevelCache.size() >= maxCachesNum) {
            // remove eldest element from the cache
            String eldest = jsonOldestCache.poll();
            jsonHighLevelCache.remove(eldest).close();
          }

          // add new entry
          cache = buildJsonProducer(standardProps);
          jsonHighLevelCache.put(userName, cache);
          jsonOldestCache.add(userName);
        }
      } else {
        // caching is disabled. Create producer for each request.
        cache = buildJsonProducer(standardProps);
      }
      return cache;
    }

    RestProducer getAvroProducer(String userName) {
      RestProducer cache;
      if (maxCachesNum > 0) {
        cache = avroHighLevelCache.get(userName);

        if (cache == null) {
          if (avroHighLevelCache.size() >= maxCachesNum) {
            // remove eldest element from the cache
            String eldest = avroOldestCache.poll();
            avroHighLevelCache.remove(eldest).close();
          }

          // add new entry
          cache = buildAvroProducer(schemaProps);
          avroHighLevelCache.put(userName, cache);
          avroOldestCache.add(userName);
        }
      } else {
        // caching is disabled. Create producer for each request.
        cache = buildAvroProducer(schemaProps);
      }
      return cache;
    }

    RestProducer getJsonSchemaProducer(String userName) {
      RestProducer cache;
      if (maxCachesNum > 0) {
        cache = jsonSchemaHighLevelCache.get(userName);

        if (cache == null) {
          if (jsonSchemaHighLevelCache.size() >= maxCachesNum) {
            // remove eldest element from the cache
            String eldest = jsonSchemaOldestCache.poll();
            jsonSchemaHighLevelCache.remove(eldest).close();
          }

          // add new entry
          cache = buildJsonSchemaProducer(schemaProps);
          jsonSchemaHighLevelCache.put(userName, cache);
          jsonSchemaOldestCache.add(userName);
        }
      } else {
        // caching is disabled. Create producer for each request.
        cache = buildJsonSchemaProducer(schemaProps);
      }
      return cache;
    }

    RestProducer getProtobufProducer(String userName) {
      RestProducer cache;
      if (maxCachesNum > 0) {
        cache = protobufHighLevelCache.get(userName);

        if (cache == null) {
          if (protobufHighLevelCache.size() >= maxCachesNum) {
            // remove eldest element from the cache
            String eldest = protobufOldestCache.poll();
            protobufHighLevelCache.remove(eldest).close();
          }

          // add new entry
          cache = buildProtobufProducer(schemaProps);
          protobufHighLevelCache.put(userName, cache);
          protobufOldestCache.add(userName);
        }
      } else {
        // caching is disabled. Create producer for each request.
        cache = buildProtobufProducer(schemaProps);
      }
      return cache;
    }

    void shutdown() {
      for (RestProducer binaryRestProducer : binaryHighLevelCache.values()) {
        binaryRestProducer.close();
      }
      for (RestProducer jsonRestProducer : jsonHighLevelCache.values()) {
        jsonRestProducer.close();
      }
      for (RestProducer avroRestProducer : avroHighLevelCache.values()) {
        avroRestProducer.close();
      }
      for (RestProducer jsonSchemaRestProducer : jsonSchemaHighLevelCache.values()) {
        jsonSchemaRestProducer.close();
      }
      for (RestProducer protobufRestProducer : protobufHighLevelCache.values()) {
        protobufRestProducer.close();
      }
    }
  }
}
