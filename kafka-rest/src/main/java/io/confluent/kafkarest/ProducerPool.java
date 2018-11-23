/*
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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import  io.confluent.rest.exceptions.RestServerErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.SchemaHolder;

/**
 * Shared pool of Kafka producers used to send messages. The pool manages batched sends, tracking
 * all required acks for a batch and managing timeouts. Currently this pool only contains one
 * producer per serialization format (e.g. byte[], Avro).
 */
public class ProducerPool {

  private static final Logger log = LoggerFactory.getLogger(ProducerPool.class);
  private Map<EmbeddedFormat, RestProducer> producers = new HashMap<>();
  private SimpleProducerCache producerCache;
  private boolean isStreams;
  private boolean defaultStreamSet;
  private boolean isImpersonationEnabled;
  private Map<String, Object> standardConfig;
  private Map<String, Object> avroConfig;

  public ProducerPool(KafkaRestConfig appConfig) {
    this(appConfig, null);
  }

  public ProducerPool(
      KafkaRestConfig appConfig,
      Properties producerConfigOverrides
  ) {
    this(appConfig, appConfig.bootstrapBrokers(), producerConfigOverrides);
  }

  public ProducerPool(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
      this.isStreams = appConfig.isStreams();
      this.defaultStreamSet = appConfig.isDefaultStreamSet();
      this.isImpersonationEnabled = appConfig.isImpersonationEnabled();

      standardConfig = buildStandardConfig(appConfig, bootstrapBrokers, producerConfigOverrides);
      avroConfig = buildAvroConfig(appConfig, bootstrapBrokers, producerConfigOverrides);

      // Initialize producers for Streams backend.
      // Avro serialization is not supported by MapR Streams.
      if(!isImpersonationEnabled){
          producers.put(EmbeddedFormat.BINARY, buildBinaryProducer(standardConfig));
          producers.put(EmbeddedFormat.JSON, buildJsonProducer(standardConfig));
      } else {
          producerCache = new SimpleProducerCache(appConfig);
      }
  }

  private Map<String, Object> buildStandardConfig(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);

    Properties producerProps = appConfig.getProducerProperties();
    // configure default stream
    String defaultStream = appConfig.getString(KafkaRestConfig.STREAMS_DEFAULT_STREAM_CONFIG);
    if (!"".equals(defaultStream)) {
        props.put(ProducerConfig.STREAMS_PRODUCER_DEFAULT_STREAM_CONFIG, defaultStream);
    }
    int streamBuffer = appConfig.getInt(KafkaRestConfig.STREAM_BUFFER_MAX_TIME_CONFIG);
    props.put(ProducerConfig.STREAMS_BUFFER_TIME_CONFIG, streamBuffer);
      
    return buildConfig(props, producerProps, producerConfigOverrides);
  }

  private NoSchemaRestProducer<byte[], byte[]> buildBinaryProducer(Map<String, Object> config) {
    return buildNoSchemaProducer(config, new ByteArraySerializer(), new ByteArraySerializer());
  }

  private NoSchemaRestProducer<Object, Object> buildJsonProducer(Map<String, Object> config) {
    return buildNoSchemaProducer(config, new KafkaJsonSerializer<>(), new KafkaJsonSerializer<>());
  }

  private <K, V> NoSchemaRestProducer<K, V> buildNoSchemaProducer(
      Map<String, Object> config,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer
  ) {
    keySerializer.configure(config, true);
    valueSerializer.configure(config, false);
    KafkaProducer<K, V> producer = new KafkaProducer<>(config, keySerializer, valueSerializer);
    return new NoSchemaRestProducer<>(producer);
  }

  private Map<String, Object> buildAvroConfig(
      KafkaRestConfig appConfig,
      String bootstrapBrokers,
      Properties producerConfigOverrides
  ) {
    Map<String, Object> avroDefaults = new HashMap<>();
    avroDefaults.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    avroDefaults.put(
        "schema.registry.url",
        appConfig.getString(KafkaRestConfig.SCHEMA_REGISTRY_URL_CONFIG)
    );

    Properties producerProps = appConfig.getProducerProperties();
    return buildConfig(avroDefaults, producerProps, producerConfigOverrides);
  }

  private AvroRestProducer buildAvroProducer(Map<String, Object> config) {
    final KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
    avroKeySerializer.configure(config, true);
    final KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer();
    avroValueSerializer.configure(config, false);
    KafkaProducer<Object, Object> avroProducer
        = new KafkaProducer<>(config, avroKeySerializer, avroValueSerializer);
    return new AvroRestProducer(avroProducer, avroKeySerializer, avroValueSerializer);
  }

  private Map<String, Object> buildConfig(
      Map<String, Object> defaults,
      Properties userProps,
      Properties overrides
  ) {
    // Note careful ordering: built-in values we look up automatically first, then configs
    // specified by user with initial KafkaRestConfig, and finally explicit overrides passed to
    // this method (only used for tests)
    Map<String, Object> config = new HashMap<>(defaults);
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
      SchemaHolder schemaHolder,
      Collection<? extends ProduceRecord<K, V>> records,
      ProduceRequestCallback callback,
      String userName
  ) {
    ProduceTask task = new ProduceTask(schemaHolder, records.size(), callback);
    log.trace("Starting produce task " + task.toString());

    RestProducer restProducer;
    if (isStreams) {
      if (!defaultStreamSet && !topic.contains(":")) {
        throw Errors.topicNotFoundException();
      }
      //we enclose it only for streams producer
      //because there can be exception due to permissions
      if (isImpersonationEnabled) {
          if (recordFormat.equals(EmbeddedFormat.BINARY)) {
             restProducer = producerCache.getBinaryProducer(userName);
          } else {
             restProducer = producerCache.getJsonProducer(userName);
          }
      } else {
          restProducer = producers.get(recordFormat);
      }

      try {
        restProducer.produce(task, topic, partition, records);
      } catch (RestServerErrorException e){
        log.warn("Producer error "+ e);
        throw Errors.topicPermissionException();
      }
    }
  }

  public void shutdown() {
    for (RestProducer restProducer : producers.values()) {
      restProducer.close();
    }
    if (producerCache != null){
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
    void onCompletion(
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

        private ConcurrentMap<String, RestProducer> binaryHighLevelCache;
        private ConcurrentMap<String, RestProducer> jsonHighLevelCache;


         SimpleProducerCache(final KafkaRestConfig config) {
            this.maxCachesNum = config.getInt(KafkaRestConfig.PRODUCERS_MAX_CACHES_NUM_CONFIG);

            this.binaryHighLevelCache = new ConcurrentHashMap<>(maxCachesNum);
            this.jsonHighLevelCache = new ConcurrentHashMap<>(maxCachesNum);

            this.binaryOldestCache = new ConcurrentLinkedQueue<>();
            this.jsonOldestCache = new ConcurrentLinkedQueue<>();
        }


         RestProducer getBinaryProducer(final String userName) {
            if (maxCachesNum > 0) {
                RestProducer cache;
                cache = binaryHighLevelCache.get(userName);


                if (cache == null) {
                    if (binaryHighLevelCache.size() >= maxCachesNum) {
                        // remove eldest element from the cache
                        String eldest = binaryOldestCache.poll();
                        binaryHighLevelCache.remove(eldest).close();
                    }

                    // add new entry
                    cache = ProducerPool.this.buildBinaryProducer(standardConfig);
                    binaryHighLevelCache.put(userName, cache);
                    binaryOldestCache.add(userName);
                }
                return cache;
            } else {
                // caching is disabled. Create producer for each request.
                return ProducerPool.this.buildBinaryProducer(standardConfig);
            }
        }

        RestProducer getJsonProducer(final String userName) {
            if (maxCachesNum > 0) {
                RestProducer cache;
                cache = jsonHighLevelCache.get(userName);

                if (cache == null) {
                    if (jsonHighLevelCache.size() >= maxCachesNum) {
                        // remove eldest element from the cache
                        String eldest = jsonOldestCache.poll();
                        jsonHighLevelCache.remove(eldest).close();
                    }

                    // add new entry
                    cache = ProducerPool.this.buildJsonProducer(standardConfig);
                    jsonHighLevelCache.put(userName, cache);
                    jsonOldestCache.add(userName);
                }
                return cache;
            } else {
                // caching is disabled. Create producer for each request.
                return ProducerPool.this.buildJsonProducer(standardConfig);
            }
        }

        void shutdown(){
            for (RestProducer binaryRestProducer : binaryHighLevelCache.values()) {
                binaryRestProducer.close();
            }
            for (RestProducer jsonRestProducer : jsonHighLevelCache.values()) {
                jsonRestProducer.close();
            }
        }

    }
}
