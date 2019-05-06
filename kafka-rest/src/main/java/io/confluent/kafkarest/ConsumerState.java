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

import io.confluent.kafkarest.entities.TopicPartitionOffset;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

/**
 * Tracks all the state for a consumer. This class is abstract in order to support multiple
 * serialization formats. Implementations must provide deserializers and a method to convert Kafka
 * MessageAndMetadata< K,V > values to ConsumerRecords that can be returned to the client (including
 * translation if the decoded Kafka consumer type and ConsumerRecord types differ).
 */
public abstract class ConsumerState<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT>
    implements AutoCloseable, ConsumerRebalanceListener {

  private static final Logger log = LoggerFactory.getLogger(ConsumerState.class);

  private AtomicBoolean isSubscribed;

  private Map<TopicPartition, OffsetAndMetadata> consumedOffsets;
  private Map<TopicPartition, OffsetAndMetadata> committedOffsets;

  private Queue<ConsumerRecord<KafkaKeyT, KafkaValueT>> recordsQueue;

  protected KafkaRestConfig config;
  private ConsumerInstanceId instanceId;
  private Consumer<KafkaKeyT, KafkaValueT> consumer;
  volatile long expiration;

  /*
  MapR comment
  Heartbeat thread uses readLock to regularly invoke poll(0)
  in order to send heartbeats and keep consumer in a group.
  When user fetches records writeLock is used as this operation
  has greater priority then sending heartbeat.
  */
  // A read/write lock on the ConsumerState allows concurrent readTopic calls, but allows
  // commitOffsets to safely lock the entire state in order to get correct information about all
  // the topic/stream's current offset state. All operations on individual TopicStates must be
  // synchronized at that level as well (so, e.g., readTopic may modify a single TopicState, but
  // only needs read access to the ConsumerState).
  private ReadWriteLock lock;

  // KafkaConsumer should perform regular heartbeat operations in order
  // to stay in a group.
  private long nextHeartbeatTime;
  private final long heartbeatDelay;
  private ConsumerHeartbeatThread heartbeatThread;

  // The last read task on this topic that failed. Allows the next read to pick up where this one
  // left off, including accounting for response size limits
  private ConsumerReadTask failedTask;


  public ConsumerState(KafkaRestConfig config, ConsumerInstanceId instanceId,
      Properties consumerProperties, ConsumerManager.ConsumerFactory consumerFactory) {

    this.config = config;
    this.instanceId = instanceId;
    this.consumer = consumerFactory.createConsumer(
        consumerProperties, getKeyDeserializer(), getValueDeserializer());
    this.expiration = config.getTime().milliseconds()
                      + config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
    this.lock = new ReentrantReadWriteLock();

    this.recordsQueue = new LinkedList<>();

    this.isSubscribed = new AtomicBoolean(false);
    this.consumedOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
    this.committedOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();

    //TODO do not hardcode default session.timeout.ms
    final int defaultSessionTimeoutMs = 30000;
    String consumerSessionTimeout = consumerProperties
        .getProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
    if (consumerSessionTimeout == null) {
      // heartbeat thread will perform poll each
      // with frequency of 1/4 session timeout.
      this.heartbeatDelay = defaultSessionTimeoutMs / 4;
    } else {
      this.heartbeatDelay = Optional.ofNullable(Long.valueOf(consumerSessionTimeout)).orElse(0L);
    }

    this.nextHeartbeatTime = 0;
    this.heartbeatThread = new ConsumerHeartbeatThread();
  }

  protected void startHeartbeatThread() {
    heartbeatThread.start();
  }

  public ConsumerInstanceId getId() {
    return instanceId;
  }

  public Consumer<KafkaKeyT, KafkaValueT> getConsumer() {
    return consumer;
  }

  /**
   * Gets the key deserializer for the Kafka consumer.
   */
  protected abstract Deserializer<KafkaKeyT> getKeyDeserializer();

  /**
   * Gets the value deserializer for the Kafka consumer.
   */
  protected abstract Deserializer<KafkaValueT> getValueDeserializer();

  /**
   * Converts a ConsumerRecord using the Kafka deserializer types into a ConsumerRecord using the
   * client's requested types. While doing so, computes the approximate size of the message in
   * bytes, which is used to track the approximate total payload size for consumer read responses to
   * determine when to trigger the response.
   */
  public abstract ConsumerRecordAndSize<ClientKeyT, ClientValueT> convertConsumerRecord(
      ConsumerRecord<KafkaKeyT, KafkaValueT> msg
  );

  /**
   * Start a read on the given topic, enabling a read lock on this ConsumerState and a full lock on
   * the ConsumerSubscriptionState.
   */
  public void startRead() {
    lock.writeLock().lock();
  }

  /**
   * Finish a read request, releasing the lock on
   * the ConsumerSubscriptionState and the read lock on this
   * ConsumerState.
   */
  public void finishRead() {
    // after finishing read change heartbeat time
    this.nextHeartbeatTime = config.getTime().milliseconds() + heartbeatDelay;
    lock.writeLock().unlock();
  }

  public ConsumerReadTask clearFailedTask() {
    ConsumerReadTask t = failedTask;
    failedTask = null;
    return t;
  }

  public void setFailedTask(ConsumerReadTask failedTask) {
    this.failedTask = failedTask;
  }

  public Map<TopicPartition, OffsetAndMetadata> getConsumedOffsets() {
    return consumedOffsets;
  }

  public List<TopicPartitionOffset> commitOffsets() {
    lock.writeLock().lock();
    try {
      List<TopicPartitionOffset> result = getOffsets(true);
      return result;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * To keep a Consumer subscribed the poll must be invoked periodically
   * to send heartbeats to the kafka back-end
   * This method should not block for a long period.
   */
  public void sendHeartbeat() {
    // if the lock cannot be acquired it means that the read operation
    // is being occurred and there is no reason to perform poll.
    if (consumer != null  // consumer is null after close()
        && isSubscribed.get()
        && config.getTime().milliseconds() >= nextHeartbeatTime
        && lock.readLock().tryLock()) {
      try {
        log.info("Consumer {} sends heartbeat.", instanceId.getInstance());
        ConsumerRecords<KafkaKeyT, KafkaValueT> records = consumer.poll(100);

        // change next heartbeat time before processing records
        this.nextHeartbeatTime = config.getTime().milliseconds() + heartbeatDelay;

        for (ConsumerRecord<KafkaKeyT, KafkaValueT> record: records) {
          recordsQueue.add(record);
        }
      } finally {
        lock.readLock().unlock();
      }
    }
  }

  public long getNextHeartbeatTime() {
    return nextHeartbeatTime;
  }

  public Queue<ConsumerRecord<KafkaKeyT, KafkaValueT>> queue() {
    return recordsQueue;
  }


  @Override
  public void close() {
    // interrupt consumer poll request
    consumer.wakeup();
    lock.writeLock().lock();
    try {
      heartbeatThread.shutdown();
      consumer.close();
      // Marks this state entry as no longer valid because the consumer group is being destroyed.
      consumer = null;
    } finally {
      lock.writeLock().lock();
    }
  }

  public boolean expired(long nowMs) {
    return expiration <= nowMs;
  }

  public void updateExpiration() {
    this.expiration = config.getTime().milliseconds()
            + config.getInt(KafkaRestConfig.CONSUMER_INSTANCE_TIMEOUT_MS_CONFIG);
  }

  public KafkaRestConfig getConfig() {
    return config;
  }

  public void setConfig(KafkaRestConfig config) {
    this.config = config;
  }

  public boolean isSubscribed() {
    return isSubscribed.get();
  }

  public Set<String> getSubscribedTopics() {
    if (!isSubscribed.get()) {
      return Collections.emptySet();
    } else {
      lock.writeLock().lock();
      try {
        return consumer.subscription();
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  public void tryToSubscribeByTopicList(List<String> topics) {
    lock.writeLock().lock();
    try {
      if (!isSubscribed.get()) {
        consumer.subscribe(topics, this);
        isSubscribed.set(true);
      } else {
        throw Errors.consumerAlreadySubscribedException();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }


  public void tryToSubscribeByTopicRegex(String regex) {
    lock.writeLock().lock();
    try {
      if (!isSubscribed.get()) {
        consumer.subscribe(Pattern.compile(regex), this);
        isSubscribed.set(true);
      } else {
        throw Errors.consumerAlreadySubscribedException();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Gets a list of TopicPartitionOffsets describing the current state of consumer offsets, possibly
   * updating the committed offset record. This method is not synchronized.
   *
   * @param doCommit if true, updates committed offsets to be the same as the consumed
   *                            offsets.
   */
  private List<TopicPartitionOffset> getOffsets(boolean doCommit) {
    List<TopicPartitionOffset> result = new Vector<TopicPartitionOffset>();
    lock.writeLock().lock();
    try {
      for (Map.Entry<TopicPartition, OffsetAndMetadata> entry: consumedOffsets.entrySet()) {
        Integer partition = entry.getKey().partition();
        Long offset = entry.getValue().offset();
        Long committedOffset = null;
        if (doCommit) {

          // committed offsets are next offsets to be fetched
          // after the commit so we are increasing them by 1.
          OffsetAndMetadata newMetadata = new OffsetAndMetadata(
              entry.getValue().offset() + 1,
              entry.getValue().metadata());

          committedOffsets.put(entry.getKey(), newMetadata);
          committedOffset = offset;
        } else {
          OffsetAndMetadata committed = committedOffsets.get(entry.getKey());
          committedOffset = committed == null ? null : committed.offset();
        }
        result.add(new TopicPartitionOffset(entry.getKey().topic(), partition, offset,
            (committedOffset == null ? -1 : committedOffset)));
      }

      if (doCommit) {
        consumer.commitSync(committedOffsets);
      }
    } finally {
      lock.writeLock().unlock();
    }
    return result;
  }


  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    for (TopicPartition tp: partitions) {
      log.info("Consumer: {} Revoked: {}-{}", instanceId.toString(), tp.topic(), tp.partition());
      consumedOffsets.remove(tp);
      committedOffsets.remove(tp);
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    for (TopicPartition tp: partitions) {
      log.info("Consumer: {} Assigned: {}-{}", instanceId.toString(), tp.topic(), tp.partition());
    }
  }

  private class ConsumerHeartbeatThread extends Thread {
    AtomicBoolean isRunning = new AtomicBoolean(true);
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    public ConsumerHeartbeatThread() {
      super("Consumer " + instanceId.getInstance() + " Heartbeat Thread");
      setDaemon(true);
    }

    @Override
    public void run() {
      while (isRunning.get()) {
        try {
          nextHeartbeatTime =
              Math.min(nextHeartbeatTime, ConsumerState.this.getNextHeartbeatTime());

          ConsumerState.this.sendHeartbeat();
          final long wait = nextHeartbeatTime - config.getTime().milliseconds();

          if (wait > 0) {
            synchronized (this) {
              wait(wait);
            }
          }
        } catch (Exception e) {
          log.warn("Heartbeat exception " + instanceId.getInstance(), e);
        }
      }
      shutdownLatch.countDown();
    }

    public void shutdown() {
      try {
        isRunning.set(false);
        this.interrupt();
        synchronized (this) {
          notify();
        }
        shutdownLatch.await();
      } catch (InterruptedException e) {
        throw new Error("Interrupted when shutting down consumer heartbeat thread.");
      }
    }
  }
}