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

package io.confluent.kafkarest.extension;

import static io.confluent.kafkarest.Errors.NOT_SUPPORTED_API_BY_STREAMS_ERROR_CODE;
import static io.confluent.kafkarest.Errors.NOT_SUPPORTED_API_BY_STREAMS_ERROR_MESSAGE;
import static io.confluent.kafkarest.KafkaRestConfig.SCHEMA_REGISTRY_ENABLE_CONFIG;

import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.rest.entities.ErrorMessage;

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
