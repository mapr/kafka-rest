package io.confluent.kafkarest.resources;


import com.mapr.streams.Admin;
import com.mapr.streams.Streams;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.KafkaStreamsMetadataObserver;
import io.confluent.kafkarest.Versions;
import io.confluent.rest.annotations.PerformanceMetric;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import java.security.PrivilegedExceptionAction;
import java.io.IOException;
import java.util.Collection;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;

@Path("/streams")
@Produces({Versions.KAFKA_V1_JSON_WEIGHTED, Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
        Versions.JSON_WEIGHTED, Versions.KAFKA_V2_JSON_WEIGHTED})
@Consumes()
public class StreamsResource {

  private final KafkaRestContext ctx;

  public StreamsResource(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @GET
  @Path("/{stream}/topics")
  @PerformanceMetric("stream.topics.list")
  public Collection<String> list(@javax.ws.rs.core.Context HttpServletRequest httpRequest, 
                                 final @PathParam("stream") String stream) throws Exception {
      
    return  (Collection<String>) runProxyQuery(new PrivilegedExceptionAction<Collection<String>>() {
        @Override
        public Collection<String> run() throws Exception {
            Configuration conf = new Configuration();
            try (Admin admin = Streams.newAdmin(conf)) {
                if (!admin.streamExists(stream)) {
                    throw Errors.streamNotFoundException();
                }
            } catch (IOException e) {
                throw Errors.kafkaErrorException(e);
            }
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
