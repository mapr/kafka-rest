/*
 * Copyright 2020 Confluent Inc.
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

import java.util.concurrent.CompletableFuture;
import com.mapr.db.exceptions.AccessDeniedException;
import io.confluent.kafkarest.Errors;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

public final class KafkaFutures {

  private KafkaFutures() {
  }

  /**
   * Returns a {@link KafkaFuture} that is completed exceptionally with the given {@code
   * exception}.
   */
  public static <T> KafkaFuture<T> failedFuture(Throwable exception) {
    KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
    future.completeExceptionally(exception);
    return future;
  }

  /**
   * Converts the given {@link KafkaFuture} to a {@link CompletableFuture}.
   */
  public static <T> CompletableFuture<T> toCompletableFuture(KafkaFuture<T> kafkaFuture) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    kafkaFuture.whenComplete(
        (value, exception) -> {
          if (exception == null) {
            completableFuture.complete(value);
          } else {
            if (exception instanceof UnknownTopicOrPartitionException) {
              exception = convertUnknownResourceException(exception);
            }
            if (exception instanceof AccessDeniedException) {
              exception = Errors.noPermissionsException();
            }
            completableFuture.completeExceptionally(exception);
          }
        });
    return completableFuture;
  }

  private static Throwable convertUnknownResourceException(Throwable e) {
    String resource = e.getMessage().split(" ")[0];
    switch (resource) {
      case "Stream":
        return Errors.streamNotFoundException();
      case "Topic":
        return Errors.topicNotFoundException();
      case "Partition":
        return Errors.partitionNotFoundException();
      default:
        return e;
    }
  }
}
