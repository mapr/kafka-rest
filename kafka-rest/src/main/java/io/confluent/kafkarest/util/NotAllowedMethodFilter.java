package io.confluent.kafkarest.util;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import java.io.IOException;

public class NotAllowedMethodFilter implements ContainerRequestFilter {

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    String httpMethod = containerRequestContext.getMethod();

    if(httpMethod.toUpperCase().equals("OPTIONS")) {
      containerRequestContext.abortWith(Response.status(Response.Status.METHOD_NOT_ALLOWED)
              .entity("{\"error_code\":405,\"message\":\"HTTP 405 Method Not Allowed\"}")
              .build());
    }
  }
}
