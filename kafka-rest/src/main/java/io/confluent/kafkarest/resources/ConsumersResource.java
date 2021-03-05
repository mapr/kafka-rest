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

import javax.validation.Valid;
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

import io.confluent.kafkarest.AvroConsumerState;
import io.confluent.kafkarest.BinaryConsumerState;
import io.confluent.kafkarest.ConsumerState;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.JsonConsumerState;
import io.confluent.kafkarest.KafkaStreamsMetadataObserver;
import io.confluent.kafkarest.UriUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.extension.SchemaRegistryEnabled;
import io.confluent.rest.annotations.PerformanceMetric;

import io.confluent.rest.impersonation.ImpersonationUtils;

@Path("/consumers")
// We include embedded formats here so you can always use these headers when interacting with
// a consumers resource. The few cases where it isn't safe are overridden per-method
@Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED_LOW, Versions.KAFKA_V1_JSON_AVRO_WEIGHTED_LOW,
           Versions.KAFKA_V1_JSON_JSON_WEIGHTED_LOW, Versions.KAFKA_V1_JSON_WEIGHTED,
           Versions.KAFKA_DEFAULT_JSON_WEIGHTED, Versions.JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON_AVRO, Versions.KAFKA_V1_JSON_JSON,
           Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON,
           Versions.GENERIC_REQUEST})
public class ConsumersResource {

  private final KafkaRestContext ctx;

  public ConsumersResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @POST
  @Valid
  @Path("/{group}")
  @PerformanceMetric("consumer.create")
  public CreateConsumerInstanceResponse createGroup(
          @javax.ws.rs.core.Context final UriInfo uriInfo,
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

    String instanceId = ctx.getConsumerManager().createConsumer(group, config);
    String instanceBaseUri = UriUtils.absoluteUriBuilder(ctx.getConfig(), uriInfo)
            .path("instances").path(instanceId).build().toString();
    return new CreateConsumerInstanceResponse(instanceId, instanceBaseUri);
  }

  @POST
  @Path("/{group}/instances/{instance}/offsets")
  @PerformanceMetric("consumer.commit")
  public void commitOffsets(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("group") String group,
                            final @PathParam("instance") String instance,
                            @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                            @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      commitOffsets(asyncResponse, group, instance);
      return null;
    }, auth, cookie);
  }

  private void commitOffsets(AsyncResponse asyncResponse, String group, String instance) {
    ctx.getConsumerManager().commitOffsets(group, instance, (offsets, e) -> {
      if (e != null) {
        asyncResponse.resume(e);
      } else {
        asyncResponse.resume(offsets);
      }
    });
  }

  @DELETE
  @Path("/{group}/instances/{instance}")
  @PerformanceMetric("consumer.delete")
  public void deleteGroup(final @PathParam("group") String group,
                          final @PathParam("instance") String instance,
                          @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                          @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      ctx.getConsumerManager().deleteConsumer(group, instance);
      return null;
    }, auth, cookie);
  }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("consumer.topic.read-binary")
  @Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED,
             Versions.KAFKA_V1_JSON_WEIGHTED,
             Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
             Versions.JSON_WEIGHTED})
  public void readTopicBinary(final @Suspended AsyncResponse asyncResponse,
                              final @PathParam("group") String group,
                              final @PathParam("instance") String instance,
                              final @PathParam("topic") String topic,
                              @QueryParam("max_bytes") @DefaultValue("-1") final long maxBytes,
                              @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                              @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      readTopic(asyncResponse, group, instance, topic, maxBytes, BinaryConsumerState.class);
      return null;
    }, auth, cookie);
  }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("consumer.topic.read-json")
  @Produces({Versions.KAFKA_V1_JSON_JSON_WEIGHTED_LOW})// Using low weight ensures binary is default
  public void readTopicJson(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("group") String group,
                            final @PathParam("instance") String instance,
                            final @PathParam("topic") String topic,
                            @QueryParam("max_bytes") @DefaultValue("-1") final long maxBytes,
                            @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                            @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      readTopic(asyncResponse, group, instance, topic, maxBytes, JsonConsumerState.class);
      return null;
    }, auth, cookie);
  }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @SchemaRegistryEnabled
  @PerformanceMetric("consumer.topic.read-avro")
  @Produces({Versions.KAFKA_V1_JSON_AVRO_WEIGHTED_LOW})// Using low weight ensures binary is default
  public void readTopicAvro(final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("group") String group,
                            final @PathParam("instance") String instance,
                            final @PathParam("topic") String topic,
                            @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
                            @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                            @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      readTopic(asyncResponse, group, instance, topic, maxBytes, AvroConsumerState.class);
      return null;
    }, auth, cookie);
  }

  private <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> void readTopic(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @PathParam("topic") String topic,
      @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
      Class<? extends ConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>>
          consumerStateType) {
    maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;
    final KafkaStreamsMetadataObserver metadataObserver = ctx.getMetadataObserver();
    String fqTopic = metadataObserver.toFullyQualifiedTopic(topic);
    try {
      ctx.getConsumerManager().readTopic(metadataObserver,
          group, instance, fqTopic, consumerStateType, maxBytes,
          (records, e) -> {
            if (e != null) {
              asyncResponse.resume(e);
            } else {
              asyncResponse.resume(records);
            }
          }
      );
    } finally {
      if (ImpersonationUtils.isImpersonationEnabled()) {
        new Thread(() -> metadataObserver.shutdown()).start();
      }
    }
  }
}
