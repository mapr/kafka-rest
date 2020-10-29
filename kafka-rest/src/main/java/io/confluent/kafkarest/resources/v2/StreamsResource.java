package io.confluent.kafkarest.resources.v2;


import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.KafkaStreamsMetadataObserver;
import io.confluent.kafkarest.Versions;
import io.confluent.rest.annotations.PerformanceMetric;
import org.apache.hadoop.security.UserGroupInformation;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;

@Path("/streams")
@Produces({Versions.KAFKA_V2_JSON_BINARY_WEIGHTED_LOW, Versions.KAFKA_V2_JSON_AVRO_WEIGHTED_LOW,
        Versions.KAFKA_V2_JSON_JSON_WEIGHTED_LOW, Versions.KAFKA_V2_JSON_WEIGHTED})
@Consumes()
public class StreamsResource {

  private final KafkaRestContext ctx;

  public StreamsResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @GET
  @Path("/{stream}/topics")
  @PerformanceMetric("stream.topics.list+v2")
  public Collection<String> list(@javax.ws.rs.core.Context HttpServletRequest httpRequest, 
                                 final @PathParam("stream") String stream) throws Exception {
      
    return  (Collection<String>) runProxyQuery(new PrivilegedExceptionAction<Collection<String>>() {
        @Override
        public Collection<String> run() throws Exception {
            final KafkaStreamsMetadataObserver metadataObserver = ctx.getMetadataObserver();
            try {
                return metadataObserver.getTopicNames(stream);
            } finally {
                if (ctx.isImpersonationEnabled()) {
                    new Thread() {
                        @Override
                        public void run() {
                            metadataObserver.shutdown();
                        }
                    }.start();
                }
            }
        }
    }, httpRequest.getRemoteUser());
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
