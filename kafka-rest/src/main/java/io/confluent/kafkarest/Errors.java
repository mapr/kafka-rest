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

import io.confluent.rest.exceptions.KafkaExceptionMapper;
import org.apache.kafka.common.config.ConfigException;

import javax.ws.rs.core.Response;

import io.confluent.rest.exceptions.RestConstraintViolationException;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;
import io.confluent.rest.exceptions.RestServerErrorException;

public class Errors {

  public static final int KAFKA_AUTHENTICATION_ERROR_CODE =
      KafkaExceptionMapper.KAFKA_AUTHENTICATION_ERROR_CODE;
  public static final int KAFKA_AUTHORIZATION_ERROR_CODE =
      KafkaExceptionMapper.KAFKA_AUTHORIZATION_ERROR_CODE;
  public static final int KAFKA_RETRIABLE_ERROR_ERROR_CODE =
      KafkaExceptionMapper.KAFKA_RETRIABLE_ERROR_ERROR_CODE;

  public static final String NO_PERMISSIONS_ERROR_MESSAGE =
          "You have no permissions to perform that action.";
  public static final int NO_PERMISSIONS_ERROR_CODE = 403001;

  public static RestException noPermissionsException() {
    return new RestException(
            NO_PERMISSIONS_ERROR_MESSAGE,
            Response.Status.FORBIDDEN.getStatusCode(),
            NO_PERMISSIONS_ERROR_CODE
    );
  }

  public static final String TOPIC_NOT_FOUND_MESSAGE = "Topic not found.";
  public static final int TOPIC_NOT_FOUND_ERROR_CODE =
      KafkaExceptionMapper.TOPIC_NOT_FOUND_ERROR_CODE;

  public static RestException topicNotFoundException() {
    return topicNotFoundException(TOPIC_NOT_FOUND_MESSAGE);
  }

  public static RestException topicNotFoundException(String message) {
    return new RestNotFoundException(message, TOPIC_NOT_FOUND_ERROR_CODE);
  }

  public static final String PARTITION_NOT_FOUND_MESSAGE = "Partition not found.";
  public static final int PARTITION_NOT_FOUND_ERROR_CODE =
      KafkaExceptionMapper.PARTITION_NOT_FOUND_ERROR_CODE;

  public static RestException partitionNotFoundException() {
    return partitionNotFoundException(PARTITION_NOT_FOUND_MESSAGE);
  }

  public static RestException partitionNotFoundException(String message) {
    return new RestNotFoundException(message, PARTITION_NOT_FOUND_ERROR_CODE);
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

  public static final String STREAM_NOT_FOUND_MESSAGE = "Stream not found.";
  public static final int STREAM_NOT_FOUND_ERROR_CODE = 40405;

  public static RestException streamNotFoundException() {
    return streamNotFoundException(STREAM_NOT_FOUND_MESSAGE);
  }

  public static RestException streamNotFoundException(String message) {
    return new RestNotFoundException(message, STREAM_NOT_FOUND_ERROR_CODE);
  }

  public static final String CLUSTER_NOT_FOUND_MESSAGE = "Cluster not found.";
  public static final int CLUSTER_NOT_FOUND_ERROR_CODE = 40406;

  public static RestException clusterNotFoundException() {
    return clusterNotFoundException(CLUSTER_NOT_FOUND_MESSAGE);
  }

  public static RestException clusterNotFoundException(String message) {
    return new RestNotFoundException(message, CLUSTER_NOT_FOUND_ERROR_CODE);
  }

  public static final String REPLICA_NOT_FOUND_MESSAGE = "Replica not found.";
  public static final int REPLICA_NOT_FOUND_ERROR_CODE = 40407;

  public static RestException replicaNotFoundException() {
    return new RestNotFoundException(
            REPLICA_NOT_FOUND_MESSAGE,
            REPLICA_NOT_FOUND_ERROR_CODE
    );
  }

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

  public static final String JSON_CONVERSION_MESSAGE = "Conversion of JSON to Object failed: ";
  public static final int JSON_CONVERSION_ERROR_CODE = 42203;

  public static RestConstraintViolationException jsonConversionException(Throwable t) {
    return new RestConstraintViolationException(
        JSON_CONVERSION_MESSAGE + t.getMessage(),
        JSON_CONVERSION_ERROR_CODE
    );

  }

  public static final String INVALID_CONSUMER_CONFIG_MESSAGE = "Invalid consumer configuration: ";
  public static final int INVALID_CONSUMER_CONFIG_ERROR_CODE = 42204;

  public static RestConstraintViolationException invalidConsumerConfigException(
      String exceptionMessage
  ) {
    return new RestConstraintViolationException(
        INVALID_CONSUMER_CONFIG_MESSAGE + exceptionMessage,
        INVALID_CONSUMER_CONFIG_ERROR_CODE
    );
  }

  public static final String INVALID_CONSUMER_CONFIG_CONSTRAINT_MESSAGE =
      "Invalid consumer configuration. It does not abide by the constraints: ";
  public static final int INVALID_CONSUMER_CONFIG_CONSTAINT_ERROR_CODE = 40001;

  public static RestConstraintViolationException invalidConsumerConfigConstraintException(
      ConfigException e
  ) {
    return new RestConstraintViolationException(
        INVALID_CONSUMER_CONFIG_CONSTRAINT_MESSAGE + e.getMessage(),
        INVALID_CONSUMER_CONFIG_CONSTAINT_ERROR_CODE
    );
  }

  public static final String INVALID_SCHEMA_MESSAGE = "Invalid schema: ";
  public static final int INVALID_SCHEMA_ERROR_CODE = 42205;

  public static RestConstraintViolationException invalidSchemaException(
      String schema
  ) {
    return new RestConstraintViolationException(
        INVALID_SCHEMA_MESSAGE + schema,
        INVALID_SCHEMA_ERROR_CODE
    );
  }

  // This is a catch-all for Kafka exceptions that can't otherwise be easily classified. For
  // producer operations this will be embedded in the per-message response. For consumer errors,
  // these are returned in the standard error format
  public static final String KAFKA_ERROR_MESSAGE = "Kafka error: ";
  public static final int KAFKA_ERROR_ERROR_CODE = KafkaExceptionMapper.KAFKA_ERROR_ERROR_CODE;

  public static RestServerErrorException kafkaErrorException(Throwable e) {
    return new RestServerErrorException(
        KAFKA_ERROR_MESSAGE + e.getMessage(),
        KAFKA_ERROR_ERROR_CODE
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

  public static final String NOT_SUPPORTED_API_BY_STREAMS_ERROR_MESSAGE =
      "MapR Streams do not currently support this API. ";
  public static final int NOT_SUPPORTED_API_BY_STREAMS_ERROR_CODE = 80001;

  public static RestConstraintViolationException notSupportedByMapRStreams() {
    return new RestConstraintViolationException(NOT_SUPPORTED_API_BY_STREAMS_ERROR_MESSAGE,
      NOT_SUPPORTED_API_BY_STREAMS_ERROR_CODE);
  }

  public static RestConstraintViolationException notSupportedByMapRStreams(String message) {
    return new RestConstraintViolationException(NOT_SUPPORTED_API_BY_STREAMS_ERROR_MESSAGE
            + message, NOT_SUPPORTED_API_BY_STREAMS_ERROR_CODE);
  }

  private static final int SERVER_LOGIN_ERROR_CODE =
          Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
  private static final String SERVER_LOGIN_ERROR_MESSAGE = "Login fails";

  public static RestServerErrorException serverLoginException(Throwable e) {
    return new RestServerErrorException(
            SERVER_LOGIN_ERROR_MESSAGE,
            SERVER_LOGIN_ERROR_CODE,
            e);
  }
}
