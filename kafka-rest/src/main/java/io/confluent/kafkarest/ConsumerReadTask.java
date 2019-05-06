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

import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.rest.exceptions.RestException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * State for tracking the progress of a single consumer read request.
 *
 * <p>To support embedded formats that require translation between the format deserialized by the
 * Kafka decoder and the format returned in the ConsumerRecord entity sent back to the client,
 * this class uses two pairs of key-value generic type parameters: KafkaK/KafkaV is the format
 * returned by the Kafka consumer's decoder/deserializer, ClientK/ClientV is the format
 * returned to the client in the HTTP response. In some cases these may be identical.
 */
class ConsumerReadTask<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>
        implements Future<List<ConsumerRecord<ClientKeyT, ClientValueT>>> {

  private static final Logger log = LoggerFactory.getLogger(ConsumerReadTask.class);

  private ConsumerState parent;
  private final long maxResponseBytes;
  private final int requestTimeoutMs;
  // the minimum bytes the task should accumulate
  // before returning a response (or hitting the timeout)
  // responseMinBytes might be bigger than maxResponseBytes
  // in cases where the functionality is disabled
  private final int responseMinBytes;
  private final ConsumerReadCallback<ClientKeyT, ClientValueT> callback;
  private CountDownLatch finished;

  private Iterator<org.apache.kafka.clients.consumer.ConsumerRecord<KafkaKeyT, KafkaValueT>> iter;
  private Consumer<KafkaKeyT, KafkaValueT> consumer;
  private List<ConsumerRecord<ClientKeyT, ClientValueT>> messages;
  private long bytesConsumed = 0;
  private final long started;
  private boolean readStarted = false;

  // Expiration if this task is waiting, considering both the expiration of the whole task and
  // a single backoff, if one is in progress
  long waitExpiration;

  public ConsumerReadTask(
          ConsumerState parent,
          String topic,
          long maxBytes,
          ConsumerReadCallback<ClientKeyT, ClientValueT> callback
  ) {
    KafkaRestConfig conf = parent.getConfig();
    this.parent = parent;
    this.maxResponseBytes = Math.min(
        maxBytes,
        conf.getLong(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG)
    );
    this.callback = callback;
    this.finished = new CountDownLatch(1);

    this.requestTimeoutMs = conf.getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
    int responseMinBytes = conf.getInt(KafkaRestConfig.PROXY_FETCH_MIN_BYTES_CONFIG);
    this.responseMinBytes = responseMinBytes < 0 ? Integer.MAX_VALUE : responseMinBytes;

    started = conf.getTime().milliseconds();
    try {

      if (!parent.isSubscribed()) {
        parent.tryToSubscribeByTopicList(Collections.singletonList(topic));
      } else {
        Set<String> actual = new HashSet<>();
        actual.add(topic);
        if (!parent.getSubscribedTopics().equals(actual)) {
          // consumer subscription does not match requested topics
          throw Errors.consumerAlreadySubscribedException();
        }
      }

      // If the previous call failed, restore any outstanding data into this task.
      ConsumerReadTask previousTask = parent.clearFailedTask();
      if (previousTask != null) {
        this.messages = previousTask.messages;
        this.bytesConsumed = previousTask.bytesConsumed;
      }
    } catch (RestException e) {
      finish(e);
    }
  }

  private boolean processConsumerRecord(org.apache.kafka.clients.consumer.ConsumerRecord record) {
    ConsumerRecordAndSize<ClientKeyT, ClientValueT> recordAndSize =
            parent.convertConsumerRecord(record);
    long roughMsgSize = recordAndSize.getSize();
    if (bytesConsumed + roughMsgSize > maxResponseBytes) {
      return false;
    } else {
      messages.add(recordAndSize.getRecord());
      bytesConsumed += roughMsgSize;
      return true;
    }
  }

  /**
   * Performs one iteration of reading from a consumer iterator.
   *
   * @return true if this read timed out, indicating the scheduler should back off
   */
  public boolean doPartialRead() {
    try {
      boolean backoff = false;

      final long startedIteration = parent.getConfig().getTime().milliseconds();
      final int requestTimeoutMs =
              parent.getConfig().getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
      final long endTime = startedIteration + requestTimeoutMs;
      final int itBackoff =
              parent.getConfig().getInt(KafkaRestConfig.CONSUMER_ITERATOR_BACKOFF_MS_CONFIG);

      // Initial setup requires locking, which must be done on this thread.
      if (consumer == null) {
        parent.startRead();
        readStarted = true;
        consumer = parent.getConsumer();
        messages = new ArrayList<>();
        // get records from queue if such exists.
        Queue<org.apache.kafka.clients.consumer.ConsumerRecord> queuedRecords = parent.queue();
        Iterator<org.apache.kafka.clients.consumer.ConsumerRecord> it = queuedRecords.iterator();
        while (it.hasNext()) {
          if (processConsumerRecord(it.next())) {
            it.remove();
          }
        }
        waitExpiration = 0;
      }

      try {
        while (parent.getConfig().getTime().milliseconds() < endTime) {

          if (iter == null || !iter.hasNext()) {
            // The consumer timeout should be set very small, so the expectation is that even in the
            // worst case, num_messages * consumer_timeout << request_timeout, so it's safe to only
            // check the elapsed time once this loop finishes.
            ConsumerRecords<KafkaKeyT, KafkaValueT> records = consumer.poll(itBackoff);
            iter = records.iterator();
            if (!iter.hasNext()) {
              // there are no records left
              backoff = true;
              break;
            }
          }

          while (iter.hasNext()) {
            org.apache.kafka.clients.consumer.ConsumerRecord record = iter.next();
            if (!processConsumerRecord(record)) {
              parent.queue().add(record);
              // add all extra records into queue.
              // They are fetched at first when the next read task happens.
              while (iter.hasNext()) {
                parent.queue().add(iter.next());
              }
              finish();
              return false;
            }
            // Updating the consumed offsets isn't done until we're actually going to return the
            // data since we may encounter an error during a subsequent read, in which case we'll
            // have to defer returning the data so we can return an HTTP error instead
          }
        }
      } catch (WakeupException cte) {
        backoff = true;
      }

      long now = parent.getConfig().getTime().milliseconds();
      long elapsed = now - started;
      // Compute backoff based on starting time. This makes reasoning about when timeouts
      // should occur simpler for tests.
      long backoffExpiration = startedIteration + itBackoff;
      long requestExpiration =
          started + parent.getConfig().getInt(KafkaRestConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
      waitExpiration = Math.min(backoffExpiration, requestExpiration);

      if (elapsed >= requestTimeoutMs) {
        finish();
      }

      return backoff;
    } catch (Exception e) {
      if (!(e instanceof RestException)) {
        e = Errors.kafkaErrorException(e);
      }
      finish((RestException) e);
      log.error("Unexpected exception in consumer read task id={} ", this, e);
      return false;
    }
  }

  public void finish() {
    finish(null);
  }

  public void finish(RestException e) {
    if (e == null) {
      // Now it's safe to mark these messages as consumed by updating offsets since we're actually
      // going to return the data.
      Map<TopicPartition, OffsetAndMetadata> consumedOffsets = parent.getConsumedOffsets();
      for (ConsumerRecord<ClientKeyT, ClientValueT> msg : messages) {
        TopicPartition topicPartition = new TopicPartition(msg.getTopic(), msg.getPartition());
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(msg.getOffset(), "");
        consumedOffsets.put(topicPartition, offsetAndMetadata);
      }
    } else {
      // If we read any messages before the exception occurred, keep this task so we don't lose
      // messages. Subsequent reads will add the outstanding messages before attempting to read
      // any more from the consumer stream iterator
      if (messages != null && messages.size() > 0) {
        parent.setFailedTask(this);
      }
    }
    if (readStarted) { // If the read is locked we need to unlock it.
      parent.finishRead();
      readStarted = false;
    }
    try {
      callback.onCompletion((e == null) ? messages : null, e);
    } catch (Throwable t) {
      // This protects the worker thread from any issues with the callback code. Nothing to be
      // done here but log it since it indicates a bug in the calling code.
      log.error("Consumer read callback threw an unhandled exception", e);
    }
    finished.countDown();
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return (finished.getCount() == 0);
  }

  @Override
  public List<ConsumerRecord<ClientKeyT, ClientValueT>> get()
          throws InterruptedException, ExecutionException {
    finished.await();
    return messages;
  }

  @Override
  public List<ConsumerRecord<ClientKeyT, ClientValueT>> get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
    finished.await(timeout, unit);
    if (finished.getCount() > 0) {
      throw new TimeoutException();
    }
    return messages;
  }
}
