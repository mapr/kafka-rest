/*
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

import static io.confluent.kafkarest.KafkaRestConfig.SCHEMA_REGISTRY_ENABLE_CONFIG;

import io.confluent.kafkarest.*;
import io.confluent.kafkarest.entities.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Vector;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import io.confluent.kafkarest.extension.SchemaRegistryEnabled;
import io.confluent.rest.annotations.PerformanceMetric;

@Path("/topics/{topic}/partitions")
@Produces({Versions.KAFKA_V2_JSON_BINARY_WEIGHTED_LOW, Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW,
           Versions.KAFKA_V2_JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V2_JSON})
public class PartitionsResource {

  private static final Logger log = LoggerFactory.getLogger(PartitionsResource.class);

  private final KafkaRestContext ctx;

  public PartitionsResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @GET
  @PerformanceMetric("partitions.list+v2")
  public List<Partition> list(@javax.ws.rs.core.Context HttpServletRequest httpRequest,
                              final @PathParam("topic") String topic) throws Exception {
    return (List<Partition>)  runProxyQuery(new PrivilegedExceptionAction() {
        @Override
        public List<Partition> run() throws Exception {
            checkTopicExists(topic);
            return ctx.getAdminClientWrapper().getTopicPartitions(topic);
        } 
    }, httpRequest.getRemoteUser());

  }

  @GET
  @Path("/{partition}")
  @PerformanceMetric("partition.get+v2")
  public Partition getPartition(
      @javax.ws.rs.core.Context HttpServletRequest httpRequest,          
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition
  ) throws Exception {
      return (Partition)  runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Partition run() throws Exception {
              checkTopicExists(topic);
              Partition part = ctx.getAdminClientWrapper().getTopicPartition(topic, partition);
              if (part == null) {
                  throw Errors.partitionNotFoundException();
              }
              return part;
          }
      }, httpRequest.getRemoteUser());      

  }


  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-binary+v2")
  @Consumes({Versions.KAFKA_V2_JSON_BINARY})
  public void produceBinary(
      final @javax.ws.rs.core.Context HttpServletRequest httpRequest,
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      final @Valid @NotNull PartitionProduceRequest<BinaryProduceRecord> request
  ) throws Exception {
       runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Partition run() throws Exception {
              produce(httpRequest.getRemoteUser(), asyncResponse, topic, partition, EmbeddedFormat.BINARY, request);
              return null;
          }
      }, httpRequest.getRemoteUser());
  }

  @POST
  @Path("/{partition}")
  @PerformanceMetric("partition.produce-json+v2")
  @Consumes({Versions.KAFKA_V2_JSON_JSON})
  public void produceJson(
      final @javax.ws.rs.core.Context HttpServletRequest httpRequest,
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      final @Valid @NotNull PartitionProduceRequest<JsonProduceRecord> request
  ) throws Exception {
      runProxyQuery(new PrivilegedExceptionAction() {
          @Override
          public Partition run() throws Exception {
              produce(httpRequest.getRemoteUser(), asyncResponse, topic, partition, EmbeddedFormat.JSON, request);
              return null;
          }
      }, httpRequest.getRemoteUser());      
  }

  @POST
  @Path("/{partition}")
  @SchemaRegistryEnabled
  @PerformanceMetric("partition.produce-avro+v2")
  @Consumes({Versions.KAFKA_V2_JSON_AVRO})
  public void produceAvro(
      final @javax.ws.rs.core.Context HttpServletRequest httpRequest,
      final @Suspended AsyncResponse asyncResponse,
      final @PathParam("topic") String topic,
      final @PathParam("partition") int partition,
      final @Valid @NotNull PartitionProduceRequest<AvroProduceRecord> request
  ) throws Exception {
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

    final String remoteUser = httpRequest.getRemoteUser();
    PrivilegedExceptionAction action = new PrivilegedExceptionAction() {
      @Override
      public Object run() {
        produce(remoteUser, asyncResponse, topic, partition, EmbeddedFormat.AVRO, request);
        return null;
      }
    };
    runProxyQuery(action, remoteUser);
  }

  protected <K, V, R extends ProduceRecord<K, V>> void produce(
      final String userName,
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

    log.trace(
        "Executing topic produce request id={} topic={} partition={} format={} request={}",
        asyncResponse, topic, partition, format, request
    );

    ctx.getProducerPool().produce(
        topic, partition, format,
        request,
        request.getRecords(),
        new ProducerPool.ProduceRequestCallback() {
          public void onCompletion(
              Integer keySchemaId, Integer valueSchemaId,
              List<RecordMetadataOrException> results
          ) {
            ProduceResponse response = new ProduceResponse();
            List<PartitionOffset> offsets = new Vector<PartitionOffset>();
            for (RecordMetadataOrException result : results) {
              if (result.getException() != null) {
                int errorCode = Errors.codeFromProducerException(result.getException());
                String errorMessage = result.getException().getMessage();
                offsets.add(new PartitionOffset(null, null, errorCode, errorMessage));
              } else {
                offsets.add(new PartitionOffset(
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    null,
                    null
                ));
              }
            }
            response.setOffsets(offsets);
            response.setKeySchemaId(keySchemaId);
            response.setValueSchemaId(valueSchemaId);
            log.trace(
                "Completed topic produce request id={} response={}",
                asyncResponse, response
            );
            asyncResponse.resume(response);
          }
        }, userName
    );
  }

  private boolean topicExists(final String topic) {
    return ctx.getAdminClientWrapper().topicExists(topic);
  }

  private void checkTopicExists(final String topic) {
    if (!topicExists(topic)) {
      throw Errors.topicNotFoundException();
    }
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
