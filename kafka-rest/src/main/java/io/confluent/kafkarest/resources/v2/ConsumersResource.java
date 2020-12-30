/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.kafkarest.resources.v2;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
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

import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.UriUtils;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerAssignmentRequest;
import io.confluent.kafkarest.entities.ConsumerAssignmentResponse;
import io.confluent.kafkarest.entities.ConsumerCommittedRequest;
import io.confluent.kafkarest.entities.ConsumerCommittedResponse;
import io.confluent.kafkarest.entities.ConsumerInstanceConfig;
import io.confluent.kafkarest.entities.ConsumerOffsetCommitRequest;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.ConsumerSeekToOffsetRequest;
import io.confluent.kafkarest.entities.ConsumerSeekToRequest;
import io.confluent.kafkarest.entities.ConsumerSubscriptionRecord;
import io.confluent.kafkarest.entities.ConsumerSubscriptionResponse;
import io.confluent.kafkarest.entities.CreateConsumerInstanceResponse;
import io.confluent.kafkarest.entities.TopicPartitionOffset;
import io.confluent.kafkarest.entities.TopicPartition;
import io.confluent.kafkarest.entities.TopicPartitionOffsetMetadata;
import io.confluent.kafkarest.extension.SchemaRegistryEnabled;
import io.confluent.kafkarest.v2.AvroKafkaConsumerState;
import io.confluent.kafkarest.v2.BinaryKafkaConsumerState;
import io.confluent.kafkarest.v2.JsonKafkaConsumerState;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import io.confluent.kafkarest.v2.KafkaConsumerState;
import io.confluent.rest.annotations.PerformanceMetric;
import org.apache.hadoop.security.UserGroupInformation;

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
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,          
      final @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      @Valid ConsumerInstanceConfig config
  ) throws Exception {
      if (config == null) {
          config = new ConsumerInstanceConfig();
      }
      final ConsumerInstanceConfig consumerConfig = config;

      return (CreateConsumerInstanceResponse) runProxyQuery(new PrivilegedExceptionAction() {
        @Override
        public CreateConsumerInstanceResponse run() throws Exception {
            String instanceId = ctx.getKafkaConsumerManager().createConsumer(group, consumerConfig);
            String instanceBaseUri = UriUtils.absoluteUriBuilder(ctx.getConfig(), uriInfo)
                    .path("instances").path(instanceId).build().toString();
            return new CreateConsumerInstanceResponse(instanceId, instanceBaseUri);
        }
    }, httpRequest.getRemoteUser());
  }

  @DELETE
  @Path("/{group}/instances/{instance}")
  @PerformanceMetric("consumer.delete+v2")
  public void deleteGroup(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance
  ) throws Exception {
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws Exception {
              ctx.getKafkaConsumerManager().deleteConsumer(group, instance);
              return null;
          }
      }, httpRequest.getRemoteUser());   
  }

  @POST
  @Path("/{group}/instances/{instance}/subscription")
  @PerformanceMetric("consumer.subscribe+v2")
  public void subscribe(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,       
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @Valid @NotNull ConsumerSubscriptionRecord subscription
  ) throws Exception {
        runProxyQuery(new PrivilegedExceptionAction() {
            @Override
            public Object run() throws Exception {
              try { 
                ctx.getKafkaConsumerManager().subscribe(group, instance, subscription);
                } catch (java.lang.IllegalStateException e) {
              throw Errors.illegalStateException(e);
              }
              return null;
            }
        }, httpRequest.getRemoteUser());
  }

  @GET
  @Path("/{group}/instances/{instance}/subscription")
  @PerformanceMetric("consumer.subscription+v2")
  public ConsumerSubscriptionResponse subscription(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance
  ) throws Exception {
      return (ConsumerSubscriptionResponse)runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public ConsumerSubscriptionResponse run() throws Exception {
              return ctx.getKafkaConsumerManager().subscription(group, instance);
          }
      }, httpRequest.getRemoteUser());
      
  }

  @DELETE
  @Path("/{group}/instances/{instance}/subscription")
  @PerformanceMetric("consumer.unsubscribe+v2")
  public void unsubscribe(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,       
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance
  ) throws Exception {
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws Exception {
              ctx.getKafkaConsumerManager().unsubscribe(group, instance);
              return null;
          }
      }, httpRequest.getRemoteUser());
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @PerformanceMetric("consumer.records.read-binary+v2")
  @Produces({Versions.KAFKA_V2_JSON_BINARY_WEIGHTED,
             Versions.KAFKA_V2_JSON_WEIGHTED})
  public void readRecordBinary(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @QueryParam("timeout") @DefaultValue("-1") long timeout,
      final @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes
  ) throws Exception {
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws Exception {
              readRecords(asyncResponse, group, instance, timeout, maxBytes, BinaryKafkaConsumerState.class);
              return null;
          }
      }, httpRequest.getRemoteUser());      
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @PerformanceMetric("consumer.records.read-json+v2")
  @Produces({Versions.KAFKA_V2_JSON_JSON_WEIGHTED_LOW})
  // Using low weight ensures binary is default
  public void readRecordJson(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,       
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @QueryParam("timeout") @DefaultValue("-1") long timeout,
      final @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes
  ) throws Exception {
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws Exception {
              readRecords(asyncResponse, group, instance, timeout, maxBytes, JsonKafkaConsumerState.class);
              return null;
          }
      }, httpRequest.getRemoteUser());
  }

  @GET
  @Path("/{group}/instances/{instance}/records")
  @SchemaRegistryEnabled
  @PerformanceMetric("consumer.records.read-avro+v2")
  @Produces({Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW})
  // Using low weight ensures binary is default
  public void readRecordAvro(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @QueryParam("timeout") @DefaultValue("-1") long timeout,
      final @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes
  ) throws Exception {
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws Exception {
              readRecords(asyncResponse, group, instance, timeout, maxBytes, AvroKafkaConsumerState.class);
              return null;
          }
      }, httpRequest.getRemoteUser());      
  }

  @POST
  @Path("/{group}/instances/{instance}/offsets")
  @PerformanceMetric("consumer.commit-offsets+v2")
  public void commitOffsets(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,       
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @QueryParam("async") @DefaultValue("false") String async,
      final @Valid ConsumerOffsetCommitRequest offsetCommitRequest
  ) throws Exception {
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws Exception {
              try {
                if (offsetCommitRequest != null) {
                  checkIfAllTopicPartitionOffsetsIsValid(offsetCommitRequest.offsets);
                }
                ctx.getKafkaConsumerManager().commitOffsets(
                        group,
                        instance,
                        async,
                        offsetCommitRequest,
                        new KafkaConsumerManager.CommitCallback() {
                            @Override
                            public void onCompletion(
                                    List<TopicPartitionOffset> offsets,
                                    Exception e
                            ) {
                                if (e != null) {
                                    asyncResponse.resume(e);
                                } else {
                                    asyncResponse.resume(offsets);
                                }
                            }
                        }
                );              return null;
              } catch (java.lang.IllegalStateException e) {
                throw Errors.illegalStateException(e);
              }   
          }
      }, httpRequest.getRemoteUser());
  }

  @GET
  @Path("/{group}/instances/{instance}/offsets")
  @PerformanceMetric("consumer.committed-offsets+v2")
  public ConsumerCommittedResponse committedOffsets(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @Valid @NotNull ConsumerCommittedRequest request
  ) throws Exception {
    if (request == null) {
      throw Errors.partitionNotFoundException();
    }
      return (ConsumerCommittedResponse)runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public ConsumerCommittedResponse run() throws Exception {
              try {
                checkIfAllTopicPartitionsIsValid(request.partitions);
                return ctx.getKafkaConsumerManager().committed(group, instance, request);
              } catch (java.lang.IllegalStateException e) {
                throw Errors.illegalStateException(e);
              }
          }
      }, httpRequest.getRemoteUser());
  }

  @POST
  @Path("/{group}/instances/{instance}/positions/beginning")
  @PerformanceMetric("consumer.seek-to-beginning+v2")
  public void seekToBeginning(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,       
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @Valid @NotNull ConsumerSeekToRequest seekToRequest
  ) throws Exception {
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws Exception {
              try {
                  if (seekToRequest != null) {
                    checkIfAllTopicPartitionsIsValid(seekToRequest.partitions);
                  }
                  ctx.getKafkaConsumerManager().seekToBeginning(group, instance, seekToRequest);
              } catch (java.lang.IllegalStateException e) {
                  throw Errors.illegalStateException(e);
              }
              return null;
          }
      }, httpRequest.getRemoteUser());      

  }

  @POST
  @Path("/{group}/instances/{instance}/positions/end")
  @PerformanceMetric("consumer.seek-to-end+v2")
  public void seekToEnd(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @Valid @NotNull ConsumerSeekToRequest seekToRequest
  ) throws Exception {
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws Exception {
              try {
                  if (seekToRequest != null) {
                    checkIfAllTopicPartitionsIsValid(seekToRequest.partitions);
                  }
                  ctx.getKafkaConsumerManager().seekToEnd(group, instance, seekToRequest);
              } catch (java.lang.IllegalStateException e) {
                  throw Errors.illegalStateException(e);
              }
              return null;
          }
      }, httpRequest.getRemoteUser());
  }

  @POST
  @Path("/{group}/instances/{instance}/positions")
  @PerformanceMetric("consumer.seek-to-offset+v2")
  public void seekToOffset(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,       
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @Valid @NotNull ConsumerSeekToOffsetRequest seekToOffsetRequest
  ) throws Exception {
          runProxyQuery(new PrivilegedExceptionAction() {
              @Override
              public Object run() throws Exception {
                  try {
                      if (seekToOffsetRequest != null) {
                        checkIfAllTopicPartitionOffsetsIsValid(seekToOffsetRequest.offsets);
                      }
                      ctx.getKafkaConsumerManager().seekToOffset(group, instance, seekToOffsetRequest);
                  } catch (java.lang.IllegalStateException e) {
                      throw Errors.illegalStateException(e);
                  }
                  return null;
              }
          }, httpRequest.getRemoteUser());      

  }

  @POST
  @Path("/{group}/instances/{instance}/assignments")
  @PerformanceMetric("consumer.assign+v2")
  public void assign(
          @javax.ws.rs.core.Context HttpServletRequest httpRequest,
          @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @Valid @NotNull ConsumerAssignmentRequest assignmentRequest
  ) throws Exception {
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws Exception {
              try {
                  ctx.getKafkaConsumerManager().assign(group, instance, assignmentRequest);
              } catch (java.lang.IllegalStateException e) {
                  throw Errors.illegalStateException(e);
              }
              return null;
          }
      }, httpRequest.getRemoteUser());
  }

  @GET
  @Path("/{group}/instances/{instance}/assignments")
  @PerformanceMetric("consumer.assignment+v2")
  public ConsumerAssignmentResponse assignment(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,
      @javax.ws.rs.core.Context UriInfo uriInfo,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance
  ) throws Exception {
      return (ConsumerAssignmentResponse)runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public ConsumerAssignmentResponse run() throws Exception {
              try {
                  return ctx.getKafkaConsumerManager().assignment(group, instance);
              } catch (java.lang.IllegalStateException e) {
                  throw Errors.illegalStateException(e);
              }
          }
      }, httpRequest.getRemoteUser());
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
        new KafkaConsumerManager.ReadCallback<ClientKeyT, ClientValueT>() {
          @Override
          public void onCompletion(
              List<? extends ConsumerRecord<ClientKeyT, ClientValueT>> records,
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

  private void checkIfStreamAndTopicExists(final String topic) {
    checkStreamExistsFromTopicName(topic);
    checkTopicExists(topic);
  }

  private void checkIfTopicAndPartitionExists(final String topic, int partition) {
    checkIfStreamAndTopicExists(topic);
    checkPartitionExists(topic, partition);
  }

  private void checkIfAllTopicPartitionOffsetsIsValid(List<TopicPartitionOffsetMetadata> offsets) {
    for (TopicPartitionOffsetMetadata offset : offsets) {
      checkIfTopicAndPartitionExists(offset.getTopic(), offset.getPartition());
    }
  }

  private void checkIfAllTopicPartitionsIsValid(List<TopicPartition> partitions) {
    for (TopicPartition partition : partitions) {
      checkIfTopicAndPartitionExists(partition.getTopic(), partition.getPartition());
    }
  }

  private void checkIfAllTopicsIsValid(List<String> topics) {
    for (String topic : topics) {
      checkIfStreamAndTopicExists(topic);
    }
  }
}
