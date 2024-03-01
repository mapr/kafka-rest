/*
 * Copyright 2020 - 2022 Confluent Inc.
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

package io.confluent.kafkarest.common;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaFutures {

  private static final Logger log = LoggerFactory.getLogger(KafkaFutures.class);

  private KafkaFutures() {}

  /**
   * Returns a {@link KafkaFuture} that is completed exceptionally with the given {@code exception}.
   */
  public static <T> KafkaFuture<T> failedFuture(Throwable exception) {
    KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
    future.completeExceptionally(exception);
    return future;
  }

  /** Converts the given {@link KafkaFuture} to a {@link CompletableFuture}. */
  public static <T> CompletableFuture<T> toCompletableFuture(KafkaFuture<T> kafkaFuture) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    kafkaFuture.whenComplete(
        (value, exception) -> {
          if (exception == null) {
            completableFuture.complete(value);
          } else {
            exception = exception.getCause() == null ? exception : exception.getCause();
            if (exception instanceof UnknownTopicOrPartitionException) {
              exception = convertUnknownResourceException(exception);
            } else if (exception instanceof KafkaException) {
              exception =
                  Errors.notSupportedByMapRStreams(
                      "Please try to set "
                          + KafkaRestConfig.STREAMS_DEFAULT_STREAM_CONFIG
                          + " to return topics for default stream");
            } else if ("com.mapr.db.exceptions.AccessDeniedException"
                .equals(exception.getClass().getName())) {
              exception = Errors.noPermissionsException();
            }
            completableFuture.completeExceptionally(exception);
          }
        });
    return completableFuture;
  }

  public static Throwable convertUnknownResourceException(Throwable e) {
    String message = e.getMessage();
    String resource = message.split(" ")[0];
    switch (resource) {
      case "Stream":
        return Errors.streamNotFoundException(message);
      case "Topic":
        return Errors.topicNotFoundException(message);
      case "Partition":
        return Errors.partitionNotFoundException(message);
      default:
        return e;
    }
  }
}
