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

package io.confluent.kafkarest.resources.v2;

import io.confluent.kafkarest.ConsumerReadCallback;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.UriUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.entities.v2.BinaryConsumerRecord;
import io.confluent.kafkarest.entities.v2.CommitOffsetsResponse;
import io.confluent.kafkarest.entities.v2.ConsumerAssignmentRequest;
import io.confluent.kafkarest.entities.v2.ConsumerAssignmentResponse;
import io.confluent.kafkarest.entities.v2.ConsumerCommittedRequest;
import io.confluent.kafkarest.entities.v2.ConsumerCommittedResponse;
import io.confluent.kafkarest.entities.v2.ConsumerOffsetCommitRequest;
import io.confluent.kafkarest.entities.v2.ConsumerSeekRequest;
import io.confluent.kafkarest.entities.v2.ConsumerSeekToRequest;
import io.confluent.kafkarest.entities.v2.ConsumerSubscriptionRecord;
import io.confluent.kafkarest.entities.v2.ConsumerSubscriptionResponse;
import io.confluent.kafkarest.entities.v2.CreateConsumerInstanceRequest;
import io.confluent.kafkarest.entities.v2.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.v2.JsonConsumerRecord;
import io.confluent.kafkarest.entities.v2.SchemaConsumerRecord;
import io.confluent.kafkarest.entities.v2.TopicPartition;
import io.confluent.kafkarest.entities.v2.TopicPartitionOffsetMetadata;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.extension.SchemaRegistryEnabled;
import io.confluent.kafkarest.v2.BinaryKafkaConsumerState;
import io.confluent.kafkarest.v2.JsonKafkaConsumerState;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import io.confluent.kafkarest.v2.KafkaConsumerState;
import io.confluent.kafkarest.v2.SchemaKafkaConsumerState;
import io.confluent.rest.annotations.PerformanceMetric;
import io.confluent.rest.impersonation.ImpersonationUtils;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import javax.ws.rs.core.UriInfo;

@Path("/consumers")
// We include embedded formats here so you can always use these headers when interacting with
// a consumers resource. The few cases where it isn't safe are overridden per-method
@Produces({
  Versions.KAFKA_V2_JSON_BINARY_WEIGHTED_LOW,
  Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW,
  Versions.KAFKA_V2_JSON_JSON_WEIGHTED_LOW,
  Versions.KAFKA_V2_JSON_JSON_SCHEMA_WEIGHTED_LOW,
  Versions.KAFKA_V2_JSON_PROTOBUF_WEIGHTED_LOW,
  Versions.KAFKA_V2_JSON_WEIGHTED
})
@Consumes({
  Versions.KAFKA_V2_JSON_BINARY,
  Versions.KAFKA_V2_JSON_AVRO,
  Versions.KAFKA_V2_JSON_JSON,
  Versions.KAFKA_V2_JSON_JSON_SCHEMA,
  Versions.KAFKA_V2_JSON_PROTOBUF,
  Versions.KAFKA_V2_JSON
})
@ResourceName("api.v2.consumers.*")
public final class ConsumersResource {

  private final Provider<KafkaRestContext> context;

  @Inject
  public ConsumersResource(Provider<KafkaRestContext> context) {
    this.context = context;
  }

  @POST
  @Valid
  @Path("/{group}")
  @PerformanceMetric("consumer.create+v2")
  @ResourceName("api.v2.consumers.create")
  public CreateConsumerInstanceResponse createGroup(
      final @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @Valid CreateConsumerInstanceRequest config,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    return ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> createGroup(uriInfo, group, config), auth, cookie);
  }

  private CreateConsumerInstanceResponse createGroup(
      UriInfo uriInfo, String group, CreateConsumerInstanceRequest config) {
    if (config == null) {
      config = CreateConsumerInstanceRequest.PROTOTYPE;
    }
    String instanceId =
        context
            .get()
            .getKafkaConsumerManager()
            .createConsumer(group, config.toConsumerInstanceConfig());
    String instanceBaseUri =
        UriUtils.absoluteUri(
            context.get().getConfig(), uriInfo, "consumers", group, "instances", instanceId);
    return new CreateConsumerInstanceResponse(instanceId, instanceBaseUri);
  }

  @DELETE
  @Path("/{group}/instances/{instance}")
  @PerformanceMetric("consumer.delete+v2")
  @ResourceName("api.v2.consumers.delete")
  public void deleteGroup(
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          context.get().getKafkaConsumerManager().deleteConsumer(group, instance);
          return null;
        },
        auth,
        cookie);
  }

  @POST
  @Path("/{group}/instances/{instance}/subscription")
  @PerformanceMetric("consumer.subscribe+v2")
  @ResourceName("api.v2.consumers.subscribe")
  public void subscribe(
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @Valid @NotNull ConsumerSubscriptionRecord subscription,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          subscribe(uriInfo, group, instance, subscription);
          return null;
        },
        auth,
        cookie);
  }

  private void subscribe(
      UriInfo uriInfo, String group, String instance, ConsumerSubscriptionRecord subscription) {
    try {
      context.get().getKafkaConsumerManager().subscribe(group, instance, subscription);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  @GET
  @Path("/{group}/instances/{instance}/subscription")
  @PerformanceMetric("consumer.subscription+v2")
  @ResourceName("api.v2.consumers.get-subscription")
  public ConsumerSubscriptionResponse subscription(
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    return ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> context.get().getKafkaConsumerManager().subscription(group, instance), auth, cookie);
  }

  @DELETE
  @Path("/{group}/instances/{instance}/subscription")
  @PerformanceMetric("consumer.unsubscribe+v2")
  @ResourceName("api.v2.consumers.unsubscribe")
  public void unsubscribe(
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          context.get().getKafkaConsumerManager().unsubscribe(group, instance);
          return null;
        },
        auth,
        cookie);
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @PerformanceMetric("consumer.records.read-binary+v2")
  @Produces({Versions.KAFKA_V2_JSON_BINARY_WEIGHTED, Versions.KAFKA_V2_JSON_WEIGHTED})
  @ResourceName("api.v2.consumers.consume-binary")
  public void readRecordBinary(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @QueryParam("timeout") @DefaultValue("-1") long timeoutMs,
      final @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          readRecords(
              asyncResponse,
              group,
              instance,
              Duration.ofMillis(timeoutMs),
              maxBytes,
              BinaryKafkaConsumerState.class,
              BinaryConsumerRecord::fromConsumerRecord);
          return null;
        },
        auth,
        cookie);
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @PerformanceMetric("consumer.records.read-json+v2")
  @Produces({Versions.KAFKA_V2_JSON_JSON_WEIGHTED_LOW})
  @ResourceName("api.v2.consumers.consume-json")
  public void readRecordJson(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @QueryParam("timeout") @DefaultValue("-1") long timeoutMs,
      final @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          readRecords(
              asyncResponse,
              group,
              instance,
              Duration.ofMillis(timeoutMs),
              maxBytes,
              JsonKafkaConsumerState.class,
              JsonConsumerRecord::fromConsumerRecord);
          return null;
        },
        auth,
        cookie);
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @SchemaRegistryEnabled
  @PerformanceMetric("consumer.records.read-avro+v2")
  @Produces({Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW})
  @ResourceName("api.v2.consumers.consume-avro")
  public void readRecordAvro(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      @QueryParam("timeout") @DefaultValue("-1") long timeoutMs,
      @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          readRecords(
              asyncResponse,
              group,
              instance,
              Duration.ofMillis(timeoutMs),
              maxBytes,
              SchemaKafkaConsumerState.class,
              SchemaConsumerRecord::fromConsumerRecord);
          return null;
        },
        auth,
        cookie);
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @PerformanceMetric("consumer.records.read-jsonschema+v2")
  @Produces({Versions.KAFKA_V2_JSON_JSON_SCHEMA_WEIGHTED_LOW})
  @ResourceName("api.v2.consumers.consume-json-schema")
  public void readRecordJsonSchema(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      @QueryParam("timeout") @DefaultValue("-1") long timeoutMs,
      @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          readRecords(
              asyncResponse,
              group,
              instance,
              Duration.ofMillis(timeoutMs),
              maxBytes,
              SchemaKafkaConsumerState.class,
              SchemaConsumerRecord::fromConsumerRecord);
          return null;
        },
        auth,
        cookie);
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @PerformanceMetric("consumer.records.read-protobuf+v2")
  @Produces({Versions.KAFKA_V2_JSON_PROTOBUF_WEIGHTED_LOW})
  @ResourceName("api.v2.consumers.consume-protobuf")
  public void readRecordProtobuf(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      @QueryParam("timeout") @DefaultValue("-1") long timeoutMs,
      @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          readRecords(
              asyncResponse,
              group,
              instance,
              Duration.ofMillis(timeoutMs),
              maxBytes,
              SchemaKafkaConsumerState.class,
              SchemaConsumerRecord::fromConsumerRecord);
          return null;
        },
        auth,
        cookie);
  }

  @POST
  @Path("/{group}/instances/{instance}/offsets")
  @PerformanceMetric("consumer.commit-offsets+v2")
  @ResourceName("api.v2.consumers.commit-offsets")
  public void commitOffsets(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @QueryParam("async") @DefaultValue("false") String async,
      final @Valid ConsumerOffsetCommitRequest offsetCommitRequest,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          commitOffsets(asyncResponse, group, instance, async, offsetCommitRequest);
          return null;
        },
        auth,
        cookie);
  }

  private void commitOffsets(
      AsyncResponse asyncResponse,
      String group,
      String instance,
      String async,
      ConsumerOffsetCommitRequest offsetCommitRequest) {
    try {
      if (offsetCommitRequest != null) {
        checkIfAllTopicPartitionOffsetsIsValidMetadata(offsetCommitRequest.getOffsets());
      }
      context
          .get()
          .getKafkaConsumerManager()
          .commitOffsets(
              group,
              instance,
              async,
              offsetCommitRequest,
              new KafkaConsumerManager.CommitCallback() {
                @Override
                public void onCompletion(List<TopicPartitionOffset> offsets, Exception e) {
                  if (e != null) {
                    asyncResponse.resume(e);
                  } else {
                    asyncResponse.resume(CommitOffsetsResponse.fromOffsets(offsets));
                  }
                }
              });
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  @GET
  @Path("/{group}/instances/{instance}/offsets")
  @PerformanceMetric("consumer.committed-offsets+v2")
  @ResourceName("api.v2.consumers.get-committed-offsets")
  public ConsumerCommittedResponse committedOffsets(
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @Valid @NotNull ConsumerCommittedRequest request,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    return ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> committedOffsets(group, instance, request), auth, cookie);
  }

  private ConsumerCommittedResponse committedOffsets(
      String group, String instance, ConsumerCommittedRequest request) {
    try {
      if (request == null) {
        throw Errors.partitionNotFoundException();
      }
      checkIfAllTopicPartitionsIsValid(request.getPartitions());
      return context.get().getKafkaConsumerManager().committed(group, instance, request);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  @POST
  @Path("/{group}/instances/{instance}/positions/beginning")
  @PerformanceMetric("consumer.seek-to-beginning+v2")
  @ResourceName("api.v2.consumers.seek-to-beginning")
  public void seekToBeginning(
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @Valid @NotNull ConsumerSeekToRequest seekToRequest,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          seekToBeginning(uriInfo, group, instance, seekToRequest);
          return null;
        },
        auth,
        cookie);
  }

  private void seekToBeginning(
      UriInfo uriInfo, String group, String instance, ConsumerSeekToRequest seekToRequest) {
    try {
      if (seekToRequest != null) {
        checkIfAllTopicPartitionsIsValid(seekToRequest.getPartitions());
      }
      context.get().getKafkaConsumerManager().seekToBeginning(group, instance, seekToRequest);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  @POST
  @Path("/{group}/instances/{instance}/positions/end")
  @PerformanceMetric("consumer.seek-to-end+v2")
  @ResourceName("api.v2.consumers.seek-to-end")
  public void seekToEnd(
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @Valid @NotNull ConsumerSeekToRequest seekToRequest,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          seekToEnd(uriInfo, group, instance, seekToRequest);
          return null;
        },
        auth,
        cookie);
  }

  private void seekToEnd(
      UriInfo uriInfo, String group, String instance, ConsumerSeekToRequest seekToRequest) {
    try {
      if (seekToRequest != null) {
        checkIfAllTopicPartitionsIsValid(seekToRequest.getPartitions());
      }
      context.get().getKafkaConsumerManager().seekToEnd(group, instance, seekToRequest);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  @POST
  @Path("/{group}/instances/{instance}/positions")
  @PerformanceMetric("consumer.seek-to-offset+v2")
  @ResourceName("api.v2.consumers.seek-to-offset")
  public void seekToOffset(
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @Valid @NotNull ConsumerSeekRequest request,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          seekToOffset(uriInfo, group, instance, request);
          return null;
        },
        auth,
        cookie);
  }

  private void seekToOffset(
      UriInfo uriInfo, String group, String instance, ConsumerSeekRequest request) {
    try {
      if (request != null) {
        checkIfAllTopicPartitionOffsetsIsValid(request.getOffsets());
      }
      context.get().getKafkaConsumerManager().seek(group, instance, request);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  @POST
  @Path("/{group}/instances/{instance}/assignments")
  @PerformanceMetric("consumer.assign+v2")
  @ResourceName("api.v2.consumers.assign")
  public void assign(
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @Valid @NotNull ConsumerAssignmentRequest assignmentRequest,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> {
          assign(uriInfo, group, instance, assignmentRequest);
          return null;
        },
        auth,
        cookie);
  }

  private void assign(
      UriInfo uriInfo, String group, String instance, ConsumerAssignmentRequest assignmentRequest) {
    try {
      context.get().getKafkaConsumerManager().assign(group, instance, assignmentRequest);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  @GET
  @Path("/{group}/instances/{instance}/assignments")
  @PerformanceMetric("consumer.assignment+v2")
  @ResourceName("api.v2.consumers.get-assignments")
  public ConsumerAssignmentResponse assignment(
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    return ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> assignment(uriInfo, group, instance), auth, cookie);
  }

  private ConsumerAssignmentResponse assignment(UriInfo uriInfo, String group, String instance) {
    try {
      return context.get().getKafkaConsumerManager().assignment(group, instance);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  private <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> void readRecords(
      final @Suspended AsyncResponse asyncResponse,
      String group,
      String instance,
      Duration timeout,
      long maxBytes,
      Class<? extends KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>>
          consumerStateType,
      Function<ConsumerRecord<ClientKeyT, ClientValueT>, ?> toJsonWrapper) {
    maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;

    context
        .get()
        .getKafkaConsumerManager()
        .readRecords(
            group,
            instance,
            consumerStateType,
            timeout,
            maxBytes,
            new ConsumerReadCallback<ClientKeyT, ClientValueT>() {
              @Override
              public void onCompletion(
                  List<ConsumerRecord<ClientKeyT, ClientValueT>> records, Exception e) {
                if (e != null) {
                  asyncResponse.resume(e);
                } else {
                  asyncResponse.resume(
                      records.stream().map(toJsonWrapper).collect(Collectors.toList()));
                }
              }
            });
  }

  private void checkIfAllTopicPartitionOffsetsIsValid(
      List<ConsumerSeekRequest.PartitionOffset> offsets) {
    offsets.forEach(
        x ->
            context
                .get()
                .getResourcesExistenceChecker()
                .checkIfTopicAndPartitionExists(x.getTopic(), x.getPartition()));
  }

  private void checkIfAllTopicPartitionOffsetsIsValidMetadata(
      List<TopicPartitionOffsetMetadata> offsets) {
    offsets.forEach(
        x ->
            context
                .get()
                .getResourcesExistenceChecker()
                .checkIfTopicAndPartitionExists(x.getTopic(), x.getPartition()));
  }

  private void checkIfAllTopicPartitionsIsValid(List<TopicPartition> partitions) {
    partitions.forEach(
        x ->
            context
                .get()
                .getResourcesExistenceChecker()
                .checkIfTopicAndPartitionExists(x.getTopic(), x.getPartition()));
  }
}
