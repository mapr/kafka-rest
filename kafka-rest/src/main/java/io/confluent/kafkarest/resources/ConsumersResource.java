/**
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

package io.confluent.kafkarest.resources;

import java.security.PrivilegedExceptionAction;

import java.util.List;
import java.util.Objects;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.UriInfo;

import io.confluent.kafkarest.AvroConsumerState;
import io.confluent.kafkarest.BinaryConsumerState;
import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.ConsumerState;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.JsonConsumerState;
import io.confluent.kafkarest.UriUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.entities.*;
import io.confluent.rest.annotations.PerformanceMetric;
import org.apache.hadoop.security.UserGroupInformation;

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
  private final boolean isStreams;

  public ConsumersResource(KafkaRestContext ctx) {
    this.ctx = ctx;
    this.isStreams = ctx.getConfig().isStreams();
  }

  @POST
  @Valid
  @Path("/{group}")
  @PerformanceMetric("consumer.create")
  public CreateConsumerInstanceResponse createGroup(@javax.ws.rs.core.Context HttpServletRequest httpRequest,
                                                    @javax.ws.rs.core.Context final UriInfo uriInfo, final @PathParam("group") String group,
                                                    @Valid ConsumerInstanceConfig config) throws Exception {

    if (config == null) {
      config = new ConsumerInstanceConfig();
    }
    final ConsumerInstanceConfig consumerConfig = config;
 
    return (CreateConsumerInstanceResponse) runProxyQuery(new PrivilegedExceptionAction() {
        @Override
        public CreateConsumerInstanceResponse run() throws Exception { 
            String instanceId = ctx.getConsumerManager().createConsumer(group, consumerConfig);
            String instanceBaseUri = UriUtils.absoluteUriBuilder(ctx.getConfig(), uriInfo)
                    .path("instances").path(instanceId).build().toString();
            return new CreateConsumerInstanceResponse(instanceId, instanceBaseUri);
                          }
    }, httpRequest.getRemoteUser()); 

  }

  @POST
  @Path("/{group}/instances/{instance}/offsets")
  @PerformanceMetric("consumer.commit")
  public void commitOffsets(@javax.ws.rs.core.Context HttpServletRequest httpRequest,
                            final @Suspended AsyncResponse asyncResponse,
                            final @PathParam("group") String group,
                            final @PathParam("instance") String instance) throws Exception {
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws Exception {
              ctx.getConsumerManager().commitOffsets(group, instance, new ConsumerManager.CommitCallback() {
                  @Override
                  public void onCompletion(List<TopicPartitionOffset> offsets, Exception e) {
                      if (e != null) {
                          asyncResponse.resume(e);
                      } else {
                          asyncResponse.resume(offsets);
                      }
                  }
              });
              return null;
          }
    }, httpRequest.getRemoteUser());
  }

  @DELETE
  @Path("/{group}/instances/{instance}")
  @PerformanceMetric("consumer.delete")
  public void deleteGroup(
      final @PathParam("group") String group,
      final @PathParam("instance") String instance
  ) {
    ctx.getConsumerManager().deleteConsumer(group, instance);
  }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("consumer.topic.read-binary")
  @Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED,
             Versions.KAFKA_V1_JSON_WEIGHTED,
             Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
             Versions.JSON_WEIGHTED})
  public void readTopicBinary( @javax.ws.rs.core.Context final HttpServletRequest httpRequest,
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @PathParam("topic") String topic,
      @QueryParam("max_bytes") @DefaultValue("-1") final long maxBytes
  ) throws Exception {  
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws Exception {
              readTopic(asyncResponse, group, instance, topic, maxBytes, BinaryConsumerState.class,
                      httpRequest.getRemoteUser());
              return null;
          }
      }, httpRequest.getRemoteUser());  }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("consumer.topic.read-json")
  @Produces({Versions.KAFKA_V1_JSON_JSON_WEIGHTED_LOW})// Using low weight ensures binary is default
  public void readTopicJson(
      @javax.ws.rs.core.Context final HttpServletRequest httpRequest,          
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @PathParam("topic") String topic,
      @QueryParam("max_bytes") @DefaultValue("-1") final long maxBytes
  ) throws Exception  {
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws Exception {
              readTopic(asyncResponse, group, instance, topic, maxBytes, JsonConsumerState.class,
                      httpRequest.getRemoteUser());
              return null;
          }
      }, httpRequest.getRemoteUser());   }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("consumer.topic.read-avro")
  @Produces({Versions.KAFKA_V1_JSON_AVRO_WEIGHTED_LOW})// Using low weight ensures binary is default
  public void readTopicAvro(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @PathParam("topic") String topic,
      @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes
  ) {
      if (isStreams) {
        throw Errors.notSupportedByMapRStreams();
    }      
    readTopic(asyncResponse, group, instance, topic, maxBytes, AvroConsumerState.class, null);
  }

  private <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> void readTopic(
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @PathParam("topic") String topic,
      @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
      Class<? extends ConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>>
          consumerStateType, String user
  ) {
    maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;
    String fqTopic = ctx.getMetadataObserver().toFullyQualifiedTopic(topic);
    
    ctx.getConsumerManager().readTopic(
        group, instance, fqTopic, consumerStateType, maxBytes,
        new ConsumerManager.ReadCallback<ClientKeyT, ClientValueT>() {
          @Override
          public void onCompletion(
              List<? extends AbstractConsumerRecord<ClientKeyT, ClientValueT>> records,
              Exception e
          ) {
            if (e != null) {
              asyncResponse.resume(e);
            } else {
              asyncResponse.resume(records);
            }
          }
        }
    );
  }

    public Object runProxyQuery(PrivilegedExceptionAction action, String remoteUser) throws Exception {
        if (ctx.isImpersonationEnabled()){
            UserGroupInformation ugi = UserGroupInformation.createProxyUser(remoteUser,
                    UserGroupInformation.getCurrentUser());
            return ugi.doAs(action);
        } else {
            return action.run();
        }
    }
}
