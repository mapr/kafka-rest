package io.confluent.kafkarest.resources;


import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.Versions;
import io.confluent.rest.annotations.PerformanceMetric;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import java.util.Collection;

import io.confluent.rest.impersonation.ImpersonationUtils;

@Path("/streams")
@Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED_LOW, Versions.KAFKA_V1_JSON_AVRO_WEIGHTED_LOW,
  Versions.KAFKA_V1_JSON_JSON_WEIGHTED_LOW, Versions.KAFKA_V1_JSON_WEIGHTED,
  Versions.KAFKA_DEFAULT_JSON_WEIGHTED, Versions.JSON_WEIGHTED})
@Consumes()
public class StreamsResource {

  private final KafkaRestContext ctx;

  public StreamsResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @GET
  @Path("/{stream}/topics")
  @PerformanceMetric("stream.topics.list")
  public Collection<String> list(final @PathParam("stream") String stream,
                                 @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
                                 @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    return ImpersonationUtils.runAsUserIfImpersonationEnabled(
        () -> ctx.getMetadataObserver().getTopicNames(stream), auth, cookie);
  }
}
