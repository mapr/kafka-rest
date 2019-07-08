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

package io.confluent.kafkarest.resources;

import io.confluent.rest.impersonation.ImpersonationUtils;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Vector;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.HttpHeaders;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.KafkaStreamsMetadataObserver;
import io.confluent.kafkarest.RecordMetadataOrException;
import io.confluent.kafkarest.SimpleConsumerManager;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.AvroProduceRecord;
import io.confluent.kafkarest.entities.BinaryProduceRecord;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.JsonProduceRecord;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionOffset;
import io.confluent.kafkarest.entities.PartitionProduceRequest;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceResponse;
import io.confluent.kafkarest.extension.SchemaRegistryEnabled;
import io.confluent.rest.annotations.PerformanceMetric;
import kafka.common.KafkaException;

@Path("/topics/{topic}/partitions")
@Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED_LOW, Versions.KAFKA_V1_JSON_AVRO_WEIGHTED_LOW,
           Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
           Versions.JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON,
           Versions.GENERIC_REQUEST})
public class PartitionsResource {

  private static final Logger log = LoggerFactory.getLogger(PartitionsResource.class);

  private final KafkaRestContext ctx;

  public PartitionsResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @GET
  @PerformanceMetric("partitions.list")
  public List<Partition> list(final @PathParam("topic") String topic,
                              @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                              @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    return ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> list(topic), auth, cookie);
  }

  private List<Partition> list(String topic) {
    checkTopicExists(topic);
    KafkaStreamsMetadataObserver metadataObserver = ctx.getMetadataObserver();
    List<Partition> partitions = metadataObserver.getTopicPartitions(topic);
    if (ctx.getConfig().isImpersonationEnabled()) {
      metadataObserver.shutdown();
    }
    return partitions;
  }

  @GET
  @Path("/{partition}")
  @PerformanceMetric("partition.get")
  public Partition getPartition(final @PathParam("topic") String topic,
                                @PathParam("partition") final int partition,
                                @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                                @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    return ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> getPartition(topic, partition), auth, cookie);
  }

  private Partition getPartition(String topic, int partition) {
    checkTopicExists(topic);
    KafkaStreamsMetadataObserver metadataObserver = ctx.getMetadataObserver();
    Partition part = metadataObserver.getTopicPartition(topic, partition);
    if (ctx.getConfig().isImpersonationEnabled()) {
      metadataObserver.shutdown();
    }
    if (part == null) {
      throw Errors.partitionNotFoundException();
    }
    return part;
  }

  @GET
  @Path("/{partition}/messages")
  @PerformanceMetric("partition.consume-binary")
  @Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED,
             Versions.KAFKA_V1_JSON_WEIGHTED,
             Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
             Versions.JSON_WEIGHTED})
  public void consumeBinary(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("topic") String topicName,
                            final @PathParam("partition") int partitionId,
                            final @QueryParam("offset") long offset,
                            final @QueryParam("count") @DefaultValue("1") long count,
                            @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                            @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      consume(asyncResponse, topicName, partitionId, offset, count, EmbeddedFormat.BINARY);
      return null;
    }, auth, cookie);
  }

  @GET
  @Path("/{partition}/messages")
  @SchemaRegistryEnabled
  @PerformanceMetric("partition.consume-avro")
  @Produces({Versions.KAFKA_V1_JSON_AVRO_WEIGHTED_LOW})
  public void consumeAvro(final @Suspended AsyncResponse asyncResponse,
                          final @PathParam("topic") String topicName,
                          final @PathParam("partition") int partitionId,
                          final @QueryParam("offset") long offset,
                          final @QueryParam("count") @DefaultValue("1") long count,
                          @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                          @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      consume(asyncResponse, topicName, partitionId, offset, count, EmbeddedFormat.AVRO);
      return null;
    }, auth, cookie);
  }

  @GET
  @Path("/{partition}/messages")
  @PerformanceMetric("partition.consume-json")
  @Produces({Versions.KAFKA_V1_JSON_JSON_WEIGHTED_LOW})
  public void consumeJson(final @Suspended AsyncResponse asyncResponse,
                          final @PathParam("topic") String topicName,
                          final @PathParam("partition") int partitionId,
                          final @QueryParam("offset") long offset,
                          final @QueryParam("count") @DefaultValue("1") long count,
                          @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                          @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      consume(asyncResponse, topicName, partitionId, offset, count, EmbeddedFormat.JSON);
      return null;
    }, auth, cookie);
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-binary")
  @Consumes({Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON,
             Versions.KAFKA_DEFAULT_JSON, Versions.JSON, Versions.GENERIC_REQUEST})
  public void produceBinary(
          final @Suspended AsyncResponse asyncResponse,
          final @PathParam("topic") String topic,
          final @PathParam("partition") int partition,
          @Valid @NotNull final PartitionProduceRequest<BinaryProduceRecord> request,
          @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
          @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      produce(asyncResponse, topic, partition, EmbeddedFormat.BINARY, request);
      return null;
    }, auth, cookie);
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-json")
  @Consumes({Versions.KAFKA_V1_JSON_JSON})
  public void produceJson(final @Suspended AsyncResponse asyncResponse,
                          final @PathParam("topic") String topic,
                          final @PathParam("partition") int partition,
                          @Valid @NotNull final PartitionProduceRequest<JsonProduceRecord> request,
                          @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                          @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      produce(asyncResponse, topic, partition, EmbeddedFormat.JSON, request);
      return null;
    }, auth, cookie);
  }

  @POST
  @Path("/{partition}")
  @SchemaRegistryEnabled
  @PerformanceMetric("partition.produce-avro")
  @Consumes({Versions.KAFKA_V1_JSON_AVRO})
  public void produceAvro(final @Suspended AsyncResponse asyncResponse,
                          final @PathParam("topic") String topic,
                          final @PathParam("partition") int partition,
                          final @Valid @NotNull PartitionProduceRequest<AvroProduceRecord> request,
                          @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                          @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    // Validations we can't do generically since they depend on the data format -- schemas need to
    // be available if there are any non-null entries
    boolean hasKeys = false;
    boolean hasValues = false;
    for (AvroProduceRecord rec : request.getRecords()) {
      hasKeys = hasKeys || !rec.getJsonKey().isNull();
      hasValues = hasValues || !rec.getJsonValue().isNull();
    }
    if (hasKeys && request.getKeySchema() == null && request.getKeySchemaId() == null) {
      throw Errors.keySchemaMissingException();
    }
    if (hasValues && request.getValueSchema() == null && request.getValueSchemaId() == null) {
      throw Errors.valueSchemaMissingException();
    }

    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      produce(asyncResponse, topic, partition, EmbeddedFormat.AVRO, request);
      return null;
    }, auth, cookie);
  }

  private <K, V> void consume(
      final @Suspended AsyncResponse asyncResponse,
      final String topicName,
      final int partitionId,
      final long offset,
      final long count,
      final EmbeddedFormat embeddedFormat
  ) {

    log.trace("Executing simple consume id={} topic={} partition={} offset={} count={}",
        asyncResponse, topicName, partitionId, offset, count
    );

    SimpleConsumerManager consumerManager = ctx.getSimpleConsumerManager();
    consumerManager.consume(
        topicName, partitionId, offset, count, embeddedFormat,
        (records, e) -> {
          log.trace(
              "Completed simple consume id={} records={} exception={}",
              asyncResponse,
              records,
              e
          );
          if (e != null) {
            asyncResponse.resume(e);
          } else {
            asyncResponse.resume(records);
          }
        }
    );

    if (ctx.getConfig().isImpersonationEnabled()) {
      consumerManager.shutdown();
    }
  }

  protected <K, V, R extends ProduceRecord<K, V>> void produce(
      final AsyncResponse asyncResponse,
      final String topic,
      final int partition,
      final EmbeddedFormat format,
      final PartitionProduceRequest<R> request
  ) {

    if (!ctx.getMetadataObserver().topicExists(topic)) {
      throw Errors.topicNotFoundException();
    }
    if (!ctx.getMetadataObserver().partitionExists(topic, partition)) {
      throw Errors.partitionNotFoundException();
    }

    log.trace("Executing topic produce request id={} topic={} partition={} format={} request={}",
        asyncResponse, topic, partition, format, request
    );

    try {
      ctx.getProducerPool().produce(
          topic, partition, format,
          request,
          request.getRecords(),
          (keySchemaId, valueSchemaId, results) -> {
            ProduceResponse response = new ProduceResponse();
            List<PartitionOffset> offsets = new Vector<PartitionOffset>();
            for (RecordMetadataOrException result : results) {
              if (result.getException() != null) {
                int errorCode = Errors.codeFromProducerException(result.getException());
                String errorMessage = result.getException().getMessage();
                offsets.add(new PartitionOffset(null, null, errorCode, errorMessage));
              } else {
                offsets.add(new PartitionOffset(result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        null, null));
              }
            }
            response.setOffsets(offsets);
            response.setKeySchemaId(keySchemaId);
            response.setValueSchemaId(valueSchemaId);
            log.trace("Completed topic produce request id={} response={}",
                    asyncResponse, response);
            asyncResponse.resume(response);
          });
    } catch (KafkaException e) {
      if (StringUtil.startsWithIgnoreCase(e.getMessage(), "Invalid partition")) {
        Errors.partitionNotFoundException();
      } else {
        Errors.kafkaErrorException(e);
      }
    }
  }

  private boolean topicExists(final String topic) {
    return ctx.getMetadataObserver().topicExists(topic);
  }

  private void checkTopicExists(final String topic) {
    if (!topicExists(topic)) {
      throw Errors.topicNotFoundException();
    }
  }
}
