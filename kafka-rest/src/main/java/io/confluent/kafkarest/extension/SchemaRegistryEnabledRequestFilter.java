package io.confluent.kafkarest.extension;

import static io.confluent.kafkarest.Errors.NOT_SUPPORTED_API_BY_STREAMS_ERROR_CODE;
import static io.confluent.kafkarest.Errors.NOT_SUPPORTED_API_BY_STREAMS_ERROR_MESSAGE;
import static io.confluent.kafkarest.KafkaRestConfig.SCHEMA_REGISTRY_ENABLE_CONFIG;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.rest.entities.ErrorMessage;
import io.confluent.rest.exceptions.RestConstraintViolationException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

@Provider
@SchemaRegistryEnabled
public class SchemaRegistryEnabledRequestFilter implements ContainerRequestFilter {

  private final KafkaRestContext ctx;

  public SchemaRegistryEnabledRequestFilter(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    boolean schemaRegistryEnabled = ctx.getConfig().getBoolean(SCHEMA_REGISTRY_ENABLE_CONFIG);
    if (!Boolean.TRUE.equals(schemaRegistryEnabled)) {
      ErrorMessage errorMessage = new ErrorMessage(
          NOT_SUPPORTED_API_BY_STREAMS_ERROR_CODE,
          NOT_SUPPORTED_API_BY_STREAMS_ERROR_MESSAGE);
      Response response = Response.status(422)
          .entity(errorMessage)
          .build();
      requestContext.abortWith(response);
    }
  }
}
