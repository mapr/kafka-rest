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

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.UriUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerAssignmentRequest;
import io.confluent.kafkarest.entities.ConsumerAssignmentResponse;
import io.confluent.kafkarest.entities.ConsumerCommittedRequest;
import io.confluent.kafkarest.entities.ConsumerCommittedResponse;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerOffsetCommitRequest;
import io.confluent.kafkarest.entities.ConsumerSeekToOffsetRequest;
import io.confluent.kafkarest.entities.ConsumerSeekToRequest;
import io.confluent.kafkarest.entities.ConsumerSubscriptionRecord;
import io.confluent.kafkarest.entities.ConsumerSubscriptionResponse;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.extension.SchemaRegistryEnabled;
import io.confluent.kafkarest.v2.AvroKafkaConsumerState;
import io.confluent.kafkarest.v2.BinaryKafkaConsumerState;
import io.confluent.kafkarest.v2.JsonKafkaConsumerState;
import io.confluent.kafkarest.v2.KafkaConsumerState;
import io.confluent.rest.annotations.PerformanceMetric;
import io.confluent.rest.impersonation.ImpersonationUtils;

@Path("/consumers")
// We include embedded formats here so you can always use these headers when interacting with
// a consumers resource. The few cases where it isn't safe are overridden per-method
@Produces(
    {
        Versions.KAFKA_V2_JSON_BINARY_WEIGHTED_LOW,
        Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW,
        Versions.KAFKA_V2_JSON_JSON_WEIGHTED_LOW,
        Versions.KAFKA_V2_JSON_WEIGHTED
    })
@Consumes(
    {
        Versions.KAFKA_V2_JSON_BINARY,
        Versions.KAFKA_V2_JSON_AVRO,
        Versions.KAFKA_V2_JSON_JSON,
        Versions.KAFKA_V2_JSON
    })
public class ConsumersResource {

  private final KafkaRestContext ctx;

  public ConsumersResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @POST
  @Valid
  @Path("/{group}")
  @PerformanceMetric("consumer.create+v2")
  public CreateConsumerInstanceResponse createGroup(
          final @javax.ws.rs.core.Context UriInfo uriInfo,
          final @PathParam("group") String group,
          final @Valid ConsumerInstanceConfig config,
          @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
          @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    return ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> createGroup(uriInfo, group, config), auth, cookie);
  }

  private CreateConsumerInstanceResponse createGroup(UriInfo uriInfo, String group,
                                                     ConsumerInstanceConfig config) {
    if (config == null) {
      config = new ConsumerInstanceConfig();
    }
    String instanceId = ctx.getKafkaConsumerManager().createConsumer(group, config);
    String instanceBaseUri = UriUtils.absoluteUriBuilder(ctx.getConfig(), uriInfo)
        .path("instances").path(instanceId).build().toString();
    return new CreateConsumerInstanceResponse(instanceId, instanceBaseUri);
  }

  @DELETE
  @Path("/{group}/instances/{instance}")
  @PerformanceMetric("consumer.delete+v2")
  public void deleteGroup(final @PathParam("group") String group,
                          final @PathParam("instance") String instance,
                          @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                          @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      ctx.getKafkaConsumerManager().deleteConsumer(group, instance);
      return null;
    }, auth, cookie);
  }

  @POST
  @Path("/{group}/instances/{instance}/subscription")
  @PerformanceMetric("consumer.subscribe+v2")
  public void subscribe(@javax.ws.rs.core.Context UriInfo uriInfo,
                        final @PathParam("group") String group,
                        final @PathParam("instance") String instance,
                        final @Valid @NotNull ConsumerSubscriptionRecord subscription,
                        @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                        @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      subscribe(uriInfo, group, instance, subscription);
      return null;
    }, auth, cookie);
  }

  private void subscribe(UriInfo uriInfo, String group, String instance,
                         ConsumerSubscriptionRecord subscription) {
    try {
      ctx.getKafkaConsumerManager().subscribe(group, instance, subscription);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  @GET
  @Path("/{group}/instances/{instance}/subscription")
  @PerformanceMetric("consumer.subscription+v2")
  public ConsumerSubscriptionResponse subscription(
          @javax.ws.rs.core.Context UriInfo uriInfo,
          final @PathParam("group") String group,
          final @PathParam("instance") String instance,
          @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
          @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    return ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> ctx.getKafkaConsumerManager().subscription(group, instance), auth, cookie);
  }

  @DELETE
  @Path("/{group}/instances/{instance}/subscription")
  @PerformanceMetric("consumer.unsubscribe+v2")
  public void unsubscribe(@javax.ws.rs.core.Context UriInfo uriInfo,
                          final @PathParam("group") String group,
                          final @PathParam("instance") String instance,
                          @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                          @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      ctx.getKafkaConsumerManager().unsubscribe(group, instance);
      return null;
    }, auth, cookie);
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @PerformanceMetric("consumer.records.read-binary+v2")
  @Produces({Versions.KAFKA_V2_JSON_BINARY_WEIGHTED,
             Versions.KAFKA_V2_JSON_WEIGHTED})
  public void readRecordBinary(final @Suspended AsyncResponse asyncResponse,
                               final @PathParam("group") String group,
                               final @PathParam("instance") String instance,
                               final @QueryParam("timeout") @DefaultValue("-1") long timeout,
                               final @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
                               @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                               @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      readRecords(asyncResponse, group, instance,
              timeout, maxBytes, BinaryKafkaConsumerState.class);
      return null;
    }, auth, cookie);
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @PerformanceMetric("consumer.records.read-json+v2")
  @Produces({Versions.KAFKA_V2_JSON_JSON_WEIGHTED_LOW})
  // Using low weight ensures binary is default
  public void readRecordJson(final @Suspended AsyncResponse asyncResponse,
                             final @PathParam("group") String group,
                             final @PathParam("instance") String instance,
                             final @QueryParam("timeout") @DefaultValue("-1") long timeout,
                             final @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
                             @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                             @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      readRecords(asyncResponse, group, instance, timeout, maxBytes, JsonKafkaConsumerState.class);
      return null;
    }, auth, cookie);
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @SchemaRegistryEnabled
  @PerformanceMetric("consumer.records.read-avro+v2")
  @Produces({Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW})
  // Using low weight ensures binary is default
  public void readRecordAvro(final @Suspended AsyncResponse asyncResponse,
                             final @PathParam("group") String group,
                             final @PathParam("instance") String instance,
                             final @QueryParam("timeout") @DefaultValue("-1") long timeout,
                             final @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
                             @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                             @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      readRecords(asyncResponse, group, instance, timeout, maxBytes, AvroKafkaConsumerState.class);
      return null;
    }, auth, cookie);
  }

  @POST
  @Path("/{group}/instances/{instance}/offsets")
  @PerformanceMetric("consumer.commit-offsets+v2")
  public void commitOffsets(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("group") String group,
                            final @PathParam("instance") String instance,
                            final @QueryParam("async") @DefaultValue("false") String async,
                            final @Valid ConsumerOffsetCommitRequest offsetCommitRequest,
                            @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                            @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      commitOffsets(asyncResponse, group, instance, async, offsetCommitRequest);
      return null;
    }, auth, cookie);
  }

  private void commitOffsets(AsyncResponse asyncResponse, String group, String instance,
                             String async, ConsumerOffsetCommitRequest offsetCommitRequest) {
    ctx.getKafkaConsumerManager().commitOffsets(
        group,
        instance,
        async,
        offsetCommitRequest,
        (offsets, e) -> {
          if (e != null) {
            asyncResponse.resume(e);
          } else {
            asyncResponse.resume(offsets);
          }
        }
    );
  }

  @GET
  @Path("/{group}/instances/{instance}/offsets")
  @PerformanceMetric("consumer.committed-offsets+v2")
  public ConsumerCommittedResponse committedOffsets(
          final @PathParam("group") String group,
          final @PathParam("instance") String instance,
          final @Valid @NotNull ConsumerCommittedRequest request,
          @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
          @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    return ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> committedOffsets(group, instance, request), auth, cookie);
  }

  private ConsumerCommittedResponse committedOffsets(String group, String instance,
                                                     ConsumerCommittedRequest request) {
    if (request == null) {
      throw Errors.partitionNotFoundException();
    }
    return ctx.getKafkaConsumerManager().committed(group, instance, request);
  }

  @POST
  @Path("/{group}/instances/{instance}/positions/beginning")
  @PerformanceMetric("consumer.seek-to-beginning+v2")
  public void seekToBeginning(@javax.ws.rs.core.Context UriInfo uriInfo,
                              final @PathParam("group") String group,
                              final @PathParam("instance") String instance,
                              final @Valid @NotNull ConsumerSeekToRequest seekToRequest,
                              @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                              @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      seekToBeginning(uriInfo, group, instance, seekToRequest);
      return null;
    }, auth, cookie);
  }

  private void seekToBeginning(UriInfo uriInfo, String group, String instance,
                               ConsumerSeekToRequest seekToRequest) {
    try {
      checkIfConsumerSeekToRequestIsValid(seekToRequest);
      ctx.getKafkaConsumerManager().seekToBeginning(group, instance, seekToRequest);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  @POST
  @Path("/{group}/instances/{instance}/positions/end")
  @PerformanceMetric("consumer.seek-to-end+v2")
  public void seekToEnd(@javax.ws.rs.core.Context UriInfo uriInfo,
                        final @PathParam("group") String group,
                        final @PathParam("instance") String instance,
                        final @Valid @NotNull ConsumerSeekToRequest seekToRequest,
                        @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                        @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      seekToEnd(uriInfo, group, instance, seekToRequest);
      return null;
    }, auth, cookie);
  }

  private void seekToEnd(UriInfo uriInfo, String group, String instance,
                         ConsumerSeekToRequest seekToRequest) {
    try {
      checkIfConsumerSeekToRequestIsValid(seekToRequest);
      ctx.getKafkaConsumerManager().seekToEnd(group, instance, seekToRequest);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  @POST
  @Path("/{group}/instances/{instance}/positions")
  @PerformanceMetric("consumer.seek-to-offset+v2")
  public void seekToOffset(@javax.ws.rs.core.Context UriInfo uriInfo,
                           final @PathParam("group") String group,
                           final @PathParam("instance") String instance,
                           final @Valid @NotNull ConsumerSeekToOffsetRequest seekToOffsetRequest,
                           @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                           @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      seekToOffset(uriInfo, group, instance, seekToOffsetRequest);
      return null;
    }, auth, cookie);
  }

  private void seekToOffset(UriInfo uriInfo, String group, String instance,
                            ConsumerSeekToOffsetRequest seekToOffsetRequest) {
    try {
      checkIfConsumerSeekToOffsetRequestIsValid(seekToOffsetRequest);
      ctx.getKafkaConsumerManager().seekToOffset(group, instance, seekToOffsetRequest);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  @POST
  @Path("/{group}/instances/{instance}/assignments")
  @PerformanceMetric("consumer.assign+v2")
  public void assign(@javax.ws.rs.core.Context UriInfo uriInfo,
                     final @PathParam("group") String group,
                     final @PathParam("instance") String instance,
                     final @Valid @NotNull ConsumerAssignmentRequest assignmentRequest,
                     @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                     @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      assign(uriInfo, group, instance, assignmentRequest);
      return null;
    }, auth, cookie);
  }

  private void assign(UriInfo uriInfo, String group, String instance,
                      ConsumerAssignmentRequest assignmentRequest) {
    try {
      ctx.getKafkaConsumerManager().assign(group, instance, assignmentRequest);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  @GET
  @Path("/{group}/instances/{instance}/assignments")
  @PerformanceMetric("consumer.assignment+v2")
  public ConsumerAssignmentResponse assignment(@javax.ws.rs.core.Context UriInfo uriInfo,
                                               final @PathParam("group") String group,
                                               final @PathParam("instance") String instance,
                                               @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                                               @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    return ImpersonationUtils.runAsUserIfImpersonationEnabled(
      () -> assignment(uriInfo, group, instance), auth, cookie);
  }

  private ConsumerAssignmentResponse assignment(UriInfo uriInfo, String group, String instance) {
    try {
      return ctx.getKafkaConsumerManager().assignment(group, instance);
    } catch (java.lang.IllegalStateException e) {
      throw Errors.illegalStateException(e);
    }
  }

  private <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> void readRecords(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      @QueryParam("timeout") @DefaultValue("-1") long timeout,
      @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
      Class<? extends KafkaConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>>
          consumerStateType
  ) {
    maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;

    ctx.getKafkaConsumerManager().readRecords(
        group, instance, consumerStateType, timeout, maxBytes,
        (records, e) -> {
          if (e != null) {
            asyncResponse.resume(e);
          } else {
            asyncResponse.resume(records);
          }
        }
    );
  }

  private boolean streamExistsFromTopicName(final String topic) {
    return ctx.getAdminClientWrapper().streamExistsFromTopicName(topic);
  }

  private boolean topicExists(final String topic) {
    return ctx.getAdminClientWrapper().topicExists(topic);
  }

  private boolean partitionExists(final String topic, int partition) {
    return ctx.getAdminClientWrapper().partitionExists(topic, partition);
  }

  private void checkStreamExistsFromTopicName(final String topic) {
    if (!streamExistsFromTopicName(topic)) {
      throw Errors.streamNotFoundException();
    }
  }

  private void checkTopicExists(final String topic) {
    if (!topicExists(topic)) {
      throw Errors.topicNotFoundException();
    }
  }

  private void checkPartitionExists(final String topic, int partition) {
    if (!partitionExists(topic, partition)) {
      throw Errors.partitionNotFoundException();
    }
  }

  private void checkIfTopicAndPartitionExists(final String topic, int partition) {
    checkStreamExistsFromTopicName(topic);
    checkTopicExists(topic);
    checkPartitionExists(topic, partition);
  }

  private void checkIfConsumerSeekToOffsetRequestIsValid(ConsumerSeekToOffsetRequest request) {
    if (request != null) {
      request.offsets.forEach(x ->
          checkIfTopicAndPartitionExists(x.getTopic(), x.getPartition()));
    }
  }

  private void checkIfConsumerSeekToRequestIsValid(ConsumerSeekToRequest seekToRequest) {
    if (seekToRequest != null) {
      seekToRequest.partitions.forEach(x ->
          checkIfTopicAndPartitionExists(x.getTopic(), x.getPartition()));
    }
  }
}
