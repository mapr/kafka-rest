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

package io.confluent.kafkarest;

import org.apache.avro.SchemaParseException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.RetriableException;

import javax.ws.rs.core.Response;

import io.confluent.rest.exceptions.RestConstraintViolationException;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;
import io.confluent.rest.exceptions.RestServerErrorException;

public class Errors {

  public static final String TOPIC_NOT_FOUND_MESSAGE = "Topic not found.";
  public static final int TOPIC_NOT_FOUND_ERROR_CODE = 40401;

  public static RestException topicNotFoundException() {
    return new RestNotFoundException(TOPIC_NOT_FOUND_MESSAGE, TOPIC_NOT_FOUND_ERROR_CODE);
  }

  public static final String PARTITION_NOT_FOUND_MESSAGE = "Partition not found.";
  public static final int PARTITION_NOT_FOUND_ERROR_CODE = 40402;

  public static RestException partitionNotFoundException() {
    return new RestNotFoundException(PARTITION_NOT_FOUND_MESSAGE, PARTITION_NOT_FOUND_ERROR_CODE);
  }

  public static final String CONSUMER_INSTANCE_NOT_FOUND_MESSAGE = "Consumer instance not found.";
  public static final int CONSUMER_INSTANCE_NOT_FOUND_ERROR_CODE = 40403;

  public static RestException consumerInstanceNotFoundException() {
    return new RestNotFoundException(
        CONSUMER_INSTANCE_NOT_FOUND_MESSAGE,
        CONSUMER_INSTANCE_NOT_FOUND_ERROR_CODE
    );
  }

  public static final String LEADER_NOT_AVAILABLE_MESSAGE = "Leader not available.";
  public static final int LEADER_NOT_AVAILABLE_ERROR_CODE = 40404;

  public static RestException leaderNotAvailableException() {
    return new RestNotFoundException(
        LEADER_NOT_AVAILABLE_MESSAGE,
        LEADER_NOT_AVAILABLE_ERROR_CODE
    );
  }

  public static RestException topicPermissionException() {
    return new RestNotFoundException(TOPIC_PERMISSION_MESSAGE,
            TOPIC_PERMISSION_MESSAGE_ERROR_CODE);
  }

  public static final String TOPIC_PERMISSION_MESSAGE =
      "Topic not found or you have not permission to access.";
  public static final int TOPIC_PERMISSION_MESSAGE_ERROR_CODE = 40405;

  public static final String CONSUMER_FORMAT_MISMATCH_MESSAGE =
      "The requested embedded data format does not match the deserializer for this consumer "
      + "instance";
  public static final int CONSUMER_FORMAT_MISMATCH_ERROR_CODE = 40601;

  public static RestException consumerFormatMismatch() {
    return new RestException(
        CONSUMER_FORMAT_MISMATCH_MESSAGE,
        Response.Status.NOT_ACCEPTABLE.getStatusCode(),
        CONSUMER_FORMAT_MISMATCH_ERROR_CODE
    );
  }


  public static final String CONSUMER_ALREADY_SUBSCRIBED_MESSAGE =
      "Consumer cannot subscribe the the specified target because it has already subscribed to "
      + "other topics.";
  public static final int CONSUMER_ALREADY_SUBSCRIBED_ERROR_CODE = 40901;

  public static RestException consumerAlreadySubscribedException() {
    return new RestException(
        CONSUMER_ALREADY_SUBSCRIBED_MESSAGE,
        Response.Status.CONFLICT.getStatusCode(),
        CONSUMER_ALREADY_SUBSCRIBED_ERROR_CODE
    );
  }

  public static final String CONSUMER_ALREADY_EXISTS_MESSAGE =
      "Consumer with specified consumer ID already exists in the specified consumer group.";
  public static final int CONSUMER_ALREADY_EXISTS_ERROR_CODE = 40902;

  public static RestException consumerAlreadyExistsException() {
    return new RestException(
        CONSUMER_ALREADY_EXISTS_MESSAGE,
        Response.Status.CONFLICT.getStatusCode(),
        CONSUMER_ALREADY_EXISTS_ERROR_CODE
    );
  }

  public static final String ILLEGAL_STATE_MESSAGE = "Illegal state: ";
  public static final int ILLEGAL_STATE_ERROR_CODE = 40903;

  public static RestException illegalStateException(Throwable t) {
    return new RestException(
        ILLEGAL_STATE_MESSAGE + t.getMessage(),
        Response.Status.CONFLICT.getStatusCode(),
        ILLEGAL_STATE_ERROR_CODE
    );
  }


  public static final String KEY_SCHEMA_MISSING_MESSAGE = "Request includes keys but does not "
                                                          + "include key schema";
  public static final int KEY_SCHEMA_MISSING_ERROR_CODE = 42201;

  public static RestConstraintViolationException keySchemaMissingException() {
    return new RestConstraintViolationException(
        KEY_SCHEMA_MISSING_MESSAGE,
        KEY_SCHEMA_MISSING_ERROR_CODE
    );

  }


  public static final String VALUE_SCHEMA_MISSING_MESSAGE = "Request includes values but does not "
                                                            + "include value schema";
  public static final int VALUE_SCHEMA_MISSING_ERROR_CODE = 42202;

  public static RestConstraintViolationException valueSchemaMissingException() {
    return new RestConstraintViolationException(
        VALUE_SCHEMA_MISSING_MESSAGE,
        VALUE_SCHEMA_MISSING_ERROR_CODE
    );

  }

  public static final String JSON_AVRO_CONVERSION_MESSAGE = "Conversion of JSON to Avro failed: ";
  public static final int JSON_AVRO_CONVERSION_ERROR_CODE = 42203;

  public static RestConstraintViolationException jsonAvroConversionException(Throwable t) {
    return new RestConstraintViolationException(
        JSON_AVRO_CONVERSION_MESSAGE + t.getMessage(),
        JSON_AVRO_CONVERSION_ERROR_CODE
    );

  }

  public static final String INVALID_CONSUMER_CONFIG_MESSAGE = "Invalid consumer configuration: ";
  public static final int INVALID_CONSUMER_CONFIG_ERROR_CODE = 42204;

  public static RestConstraintViolationException invalidConsumerConfigException(
          RuntimeException e
  ) {
    return new RestConstraintViolationException(
        INVALID_CONSUMER_CONFIG_MESSAGE + e.getMessage(),
        INVALID_CONSUMER_CONFIG_ERROR_CODE
    );
  }

  public static RestConstraintViolationException invalidConsumerConfigException(
      ConfigException e
  ) {
    return new RestConstraintViolationException(
        INVALID_CONSUMER_CONFIG_MESSAGE + e.getMessage(),
        INVALID_CONSUMER_CONFIG_ERROR_CODE
    );
  }

  public static final String INVALID_CONSUMER_CONFIG_CONSTRAINT_MESSAGE =
      "Invalid consumer configuration. It does not abide by the constraints: ";
  public static final int INVALID_CONSUMER_CONFIG_CONSTAINT_ERROR_CODE = 40001;

  public static RestConstraintViolationException invalidConsumerConfigConstraintException(
      io.confluent.common.config.ConfigException e
  ) {
    return new RestConstraintViolationException(
        INVALID_CONSUMER_CONFIG_CONSTRAINT_MESSAGE + e.getMessage(),
        INVALID_CONSUMER_CONFIG_CONSTAINT_ERROR_CODE
    );
  }

  public static final String INVALID_SCHEMA_MESSAGE = "Invalid schema: ";
  public static final int INVALID_SCHEMA_ERROR_CODE = 42205;

  public static RestConstraintViolationException invalidSchemaException(
      SchemaParseException e
  ) {
    return new RestConstraintViolationException(
        INVALID_SCHEMA_MESSAGE + e.getMessage(),
        INVALID_SCHEMA_ERROR_CODE
    );
  }

  public static final String ZOOKEEPER_ERROR_MESSAGE = "Zookeeper error: ";
  public static final int ZOOKEEPER_ERROR_ERROR_CODE = 50001;

  // This is a catch-all for Kafka exceptions that can't otherwise be easily classified. For
  // producer operations this will be embedded in the per-message response. For consumer errors,
  // these are returned in the standard error format
  public static final String KAFKA_ERROR_MESSAGE = "Kafka error: ";
  public static final int KAFKA_ERROR_ERROR_CODE = 50002;

  public static RestServerErrorException kafkaErrorException(Throwable e) {
    return new RestServerErrorException(
        KAFKA_ERROR_MESSAGE + e.getMessage(),
        KAFKA_ERROR_ERROR_CODE
    );
  }

  public static final String KAFKA_RETRIABLE_ERROR_MESSAGE = "Retriable Kafka error: ";
  public static final int KAFKA_RETRIABLE_ERROR_ERROR_CODE = 50003;

  public static RestServerErrorException kafkaRetriableErrorException(Throwable e) {
    return new RestServerErrorException(
        KAFKA_RETRIABLE_ERROR_MESSAGE + e.getMessage(),
        KAFKA_RETRIABLE_ERROR_ERROR_CODE
    );
  }

  public static final String NO_SSL_SUPPORT_MESSAGE =
      "Only SSL endpoints were found for the broker, but SSL is not currently supported.";
  public static final int NO_SSL_SUPPORT_ERROR_CODE = 50101;

  public static RestServerErrorException noSslSupportException() {
    return new RestServerErrorException(
        NO_SSL_SUPPORT_MESSAGE,
        NO_SSL_SUPPORT_ERROR_CODE
    );
  }

  public static final String NO_SIMPLE_CONSUMER_AVAILABLE_ERROR_MESSAGE =
      "No SimpleConsumer is available at the time in the pool. The request can be retried. "
      + "You can increase the pool size or the pool timeout to avoid this error in the future.";
  public static final int NO_SIMPLE_CONSUMER_AVAILABLE_ERROR_CODE = 50301;

  public static RestServerErrorException simpleConsumerPoolTimeoutException() {
    return new RestServerErrorException(
        NO_SIMPLE_CONSUMER_AVAILABLE_ERROR_MESSAGE,
        NO_SIMPLE_CONSUMER_AVAILABLE_ERROR_CODE
    );
  }

  public static final String UNEXPECTED_PRODUCER_EXCEPTION
      = "Unexpected non-Kafka exception returned by Kafka";

  public static int codeFromProducerException(Throwable e) {
    if (e instanceof RetriableException) {
      return KAFKA_RETRIABLE_ERROR_ERROR_CODE;
    } else if (e instanceof KafkaException) {
      return KAFKA_ERROR_ERROR_CODE;
    } else {
      // We shouldn't see any non-Kafka exceptions, but this covers us in case we do see an
      // unexpected error. In that case we fail the entire request -- this loses information
      // since some messages may have been produced correctly, but is the right thing to do from
      // a REST perspective since there was an internal error with the service while processing
      // the request.
      throw new RestServerErrorException(UNEXPECTED_PRODUCER_EXCEPTION,
                                         RestServerErrorException.DEFAULT_ERROR_CODE, e
      );
    }
  }

  public static final String NOT_SUPPORTED_API_BY_STREAMS_ERROR_MESSAGE =
      "MapR Streams do not currently support this API ";
  public static final int NOT_SUPPORTED_API_BY_STREAMS_ERROR_CODE = 80001;

  public static RestConstraintViolationException notSupportedByMapRStreams() {
    return new RestConstraintViolationException(NOT_SUPPORTED_API_BY_STREAMS_ERROR_MESSAGE,
      NOT_SUPPORTED_API_BY_STREAMS_ERROR_CODE);
  }

  public static RestConstraintViolationException notSupportedByMapRStreams(String message) {
    return new RestConstraintViolationException(NOT_SUPPORTED_API_BY_STREAMS_ERROR_MESSAGE
            + message, NOT_SUPPORTED_API_BY_STREAMS_ERROR_CODE);
  }

}
