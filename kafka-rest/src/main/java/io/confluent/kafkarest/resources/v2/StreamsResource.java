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

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.controllers.StreamManager;
import io.confluent.kafkarest.entities.Topic;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.rest.annotations.PerformanceMetric;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.HttpHeaders;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import io.confluent.rest.impersonation.ImpersonationUtils;
import static java.util.Objects.requireNonNull;

@Path("/streams")
@Consumes({Versions.KAFKA_V2_JSON})
@Produces({Versions.KAFKA_V2_JSON})
public class StreamsResource {

  private final Provider<StreamManager> streamManagerProvider;

  @Inject
  public StreamsResource(
          Provider<StreamManager> streamManagerProvider) {
    this.streamManagerProvider = requireNonNull(streamManagerProvider);
  }

  @GET
  @Path("/{stream}/topics")
  @PerformanceMetric("stream.topics.list+v2")
  public void list(
      @Suspended AsyncResponse asyncResponse, @PathParam("stream") String stream,
      @HeaderParam(HttpHeaders.AUTHORIZATION) String auth,
      @HeaderParam(HttpHeaders.COOKIE) String cookie) {
    ImpersonationUtils.runAsUserIfImpersonationEnabled(() -> {
      list(asyncResponse, stream);
      return null;
    }, auth, cookie);
  }

  private void list(AsyncResponse asyncResponse, String stream) {
    StreamManager streamManager = streamManagerProvider.get();

    CompletableFuture<List<String>> response =
        streamManager.listStreamTopics(stream)
            .thenApply(topics -> topics.stream().map(Topic::getName).collect(Collectors.toList()));

    AsyncResponses.asyncResume(asyncResponse, response);
  }
}

