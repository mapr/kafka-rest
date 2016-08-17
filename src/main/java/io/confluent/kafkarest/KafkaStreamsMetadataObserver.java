/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observes metadata about the Kafka cluster and MapR streams.
 */
public class KafkaStreamsMetadataObserver extends MetadataObserver {

  private static final Logger log = LoggerFactory.getLogger(KafkaStreamsMetadataObserver.class);

  private StreamsMetadataConsumer streamsMetadataConsumer;

  /**
   * There are two back-ends supported:
   * 1) Streams - In this mode only MapR Streams
   * topics can be used. If default streams was set
   * the user can define topics without stream name
   * (no ":" symbol) and those topics are considered
   * as topics within default streams.
   *
   * 2) Hybrid - if default stream is set then only Streams
   * topics are used. Otherwise topics with ":" are requested to Streams
   * and others to Kafka.
   *
   */
  private boolean isStreams;
  private boolean defaultStreamSet;
  private String defaultStream;

  public KafkaStreamsMetadataObserver(KafkaRestConfig config, ZkUtils zkUtils, boolean isStreams) {
    super(config, zkUtils);

    String bootstrapServers = config.getString(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG);
    String defaultStream =  config.getString(KafkaRestConfig.STREAMS_DEFAULT_STREAM_CONFIG);

    this.defaultStreamSet = config.isDefaultStreamSet();
    this.defaultStream = defaultStream;
    this.isStreams = isStreams;

    streamsMetadataConsumer = new StreamsMetadataConsumer(bootstrapServers, defaultStream);
  }

  @Override
  public List<Integer> getBrokerIds() {
    if (isStreams) {
      throw Errors.notSupportedByMapRStreams();
    }
    return super.getBrokerIds();
  }

  @Override
  public Broker getLeader(final String topicName, final int partitionId) {
    if (isStreams) {
      throw Errors.notSupportedByMapRStreams();
    }
    return super.getLeader(topicName, partitionId);
  }

  @Override
  public Collection<String> getTopicNames() {
    if (isStreams || defaultStreamSet) {
      try {
        return streamsMetadataConsumer.listTopics().keySet();
      } catch (KafkaException e) {
        log.warn("listTopics() API", e);
        throw Errors.notSupportedByMapRStreams(
          "Please try to set " + KafkaRestConfig.STREAMS_DEFAULT_STREAM_CONFIG
            + " to return topics for default stream");
      }
    } else {
      return super.getTopicNames();
    }
  }

  /**
   * Streams-specific
   */
  public Collection<String> getTopicNames(String stream) {
    return streamsMetadataConsumer.listTopics(stream).keySet();
  }

  public String toFullyQualifiedTopic(String topic) {
    if (defaultStreamSet && !topic.contains(":")) {
      String fullName = defaultStream;
      if (!fullName.endsWith(":")) {
        fullName = fullName.concat(":");
      }
      return fullName.concat(topic);
    }
    // in case of Kafka backend there is no need to modify topic
    return topic;
  }

  @Override
  public List<Topic> getTopics() {
    if (isStreams || defaultStreamSet) {
      try {
        List<Topic> result = new ArrayList<>();
        java.util.Map<String, List<PartitionInfo>> topicsMap = streamsMetadataConsumer.listTopics();
        for (java.util.Map.Entry<String, List<PartitionInfo>> entry: topicsMap.entrySet()) {

          Topic topic = new Topic(entry.getKey(), null, convertPartitions(entry.getValue()));
          result.add(topic);
        }
        return result;
      } catch (KafkaException e) {
        log.warn("listTopics() API", e);
        throw Errors.notSupportedByMapRStreams(
          "Please try to set " + KafkaRestConfig.STREAMS_DEFAULT_STREAM_CONFIG
            + " to return topics for default stream");
      }
    } else {
      return super.getTopics();
    }
  }

  /**
   * Streams-specific
   */
  public List<Topic> getTopics(String stream) {
    List<Topic> result = new ArrayList<>();
    java.util.Map<String, List<PartitionInfo>> topicsMap = streamsMetadataConsumer.listTopics(stream);
    for (java.util.Map.Entry<String, List<PartitionInfo>> entry: topicsMap.entrySet()) {

      Topic topic = new Topic(entry.getKey(), null, convertPartitions(entry.getValue()));
      result.add(topic);
    }
    return result;
  }

  @Override
  public boolean topicExists(String topicName) {
    if (isStreams || defaultStreamSet) {
      // Check whether topic exists in streams

      if (topicName.startsWith("/") && topicName.contains(":")) {
        String[] splitted = topicName.split(":");
        String stream = splitted[0];
        return getTopicNames(stream).contains(topicName);
      } else {
        return getTopicNames().contains(toFullyQualifiedTopic(topicName));
      }
    } else {
      if (topicName.startsWith("/") && topicName.contains(":")) {
        String[] splitted = topicName.split(":");
        String stream = splitted[0];
        return getTopicNames(stream).contains(topicName);
      } else {
        // Check whether topic exists in kafka
        return super.getTopicNames().contains(topicName);
      }
    }
  }

  /**
   * Check if request for a given topic will be sent to MapR Streams
   */
  public boolean requestToStreams(String topic) {
    return isStreams || defaultStreamSet || topic.startsWith("/") && topic.contains(":");
  }

  @Override
  public Topic getTopic(String topicName) {
    if (isStreams || defaultStreamSet) {
      return new Topic(topicName, null, getTopicPartitions(topicName));
    } else {
      if (topicName.startsWith("/") && topicName.contains(":")) {
        return new Topic(topicName, null, getTopicPartitions(topicName));
      } else {
        return super.getTopic(topicName);
      }
    }
  }

  @Override
  public List<Partition> getTopicPartitions(String topic) {
    if (isStreams || defaultStreamSet) {
      return convertPartitions(streamsMetadataConsumer.partitionsFor(topic));
    } else {
      if (topic.startsWith("/") && topic.contains(":")) {
        return convertPartitions(streamsMetadataConsumer.partitionsFor(topic));
      } else {
        return super.getTopicPartitions(topic);
      }
    }
  }

  @Override
  public boolean partitionExists(String topicName, int partition) {
    if (isStreams || defaultStreamSet) {
      return (partition >= 0 && partition < streamsMetadataConsumer.partitionsFor(topicName).size());
    } else {
      if (topicName.startsWith("/") && topicName.contains(":")) {
        return (partition >= 0 && partition < streamsMetadataConsumer.partitionsFor(topicName).size());
      } else {
        return super.partitionExists(topicName, partition);
      }
    }
  }

  @Override
  public Partition getTopicPartition(String topic, int partition) {
    List<Partition> partitions = getTopicPartitions(topic);
    for (Partition p: partitions) {
      if (p.getPartition() == partition) {
        return p;
      }
    }
    return null;
  }

  @Override
  public int getLeaderId(final String topicName, final int partitionId) {
    if (isStreams) {
      throw Errors.notSupportedByMapRStreams();
    } else {
      return super.getLeaderId(topicName, partitionId);
    }
  }

  @Override
  public void shutdown() {
    log.debug("Shutting down MetadataObserver");
    try {
      if (streamsMetadataConsumer != null) {
        streamsMetadataConsumer.close();
      }
      super.shutdown();
    } catch (Exception e) {
      log.error("Close metadata consumer", e);
    }
  }

  private static List<Partition> convertPartitions(List<PartitionInfo> partitionInfos) {
    List<Partition> partitions = new ArrayList<>();
    for(PartitionInfo partitionInfo: partitionInfos) {
      List<PartitionReplica> replicas = new ArrayList<>();

      // the leader replica is always in sync
      replicas.add(new PartitionReplica(partitionInfo.leader().id(), true, true));

      List<Node> inSyncReplicas = Arrays.asList(partitionInfo.inSyncReplicas());
      for (Node node: partitionInfo.replicas()) {
        replicas.add(new PartitionReplica(node.id(), false, inSyncReplicas.contains(node)));
      }
      Partition partition =
        new Partition(partitionInfo.partition(), partitionInfo.leader().id(), replicas);
      partitions.add(partition);
    }
    return partitions;
  }
}


/**
 * Synchronized wrapper for KafkaConsumer that
 * is used to fetch metadata about topics for
 * both MapR Streams and Kafka.
 *
 * Note: for Kafka some APIs are unavailable
 * and en exception is thrown.
 */
class StreamsMetadataConsumer implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(StreamsMetadataConsumer.class);

  private KafkaConsumer<byte[], byte[]> metadataConsumer;
  private Lock consumerLock;

  StreamsMetadataConsumer(String bootstrapServers, String defaultStream) {
    Properties properties = new Properties();
    if (bootstrapServers != null) {
      properties.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }
    boolean defaultStreamProvided = false;
    if (!"".equals(defaultStream)) {
      properties.setProperty(
        ConsumerConfig.STREAMS_CONSUMER_DEFAULT_STREAM_CONFIG, defaultStream);
      defaultStreamProvided = true;
    }
    metadataConsumer = new KafkaConsumer<byte[], byte[]>(properties,
      new ByteArrayDeserializer(), new ByteArrayDeserializer());

    if (!defaultStreamProvided) {
      // We need to have initialized KafkaConsumer driver right after
      // creation if default stream was not specified.
      // This forces consumer initialization in Streams mode.
      try {
        Method method = metadataConsumer.getClass().getDeclaredMethod("initializeConsumer", String.class);
        method.setAccessible(true);
        method.invoke(metadataConsumer, "/:");
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        log.warn("Metadata consumer error: ", e);
      }
    }

    this.consumerLock = new ReentrantLock(true);
  }

  /**
   * Available for MapR Streams
   * @return list of topics for a specific stream
   */
  java.util.Map<String, List<PartitionInfo>> listTopics(String stream) {
    consumerLock.lock();
    try {
      return metadataConsumer.listTopics(stream);
    } finally {
      consumerLock.unlock();
    }
  }

  /**
   * @return all kafka topics in case of Kafka backend or
   * topics in default stream in case of MapR Streams backend.
   */
  java.util.Map<String, List<PartitionInfo>> listTopics() {
    consumerLock.lock();
    try {
      return metadataConsumer.listTopics();
    } finally {
      consumerLock.unlock();
    }
  }

  List<PartitionInfo> partitionsFor(String topic) {
    consumerLock.lock();
    try {
      return metadataConsumer.partitionsFor(topic);
    } catch (UnknownTopicOrPartitionException e) {
      throw Errors.topicNotFoundException();
    } finally {
      consumerLock.unlock();
    }
  }


  @Override
  public void close() throws Exception {
    metadataConsumer.wakeup();
    consumerLock.lock();
    try {
      metadataConsumer.close();
    } finally {
      consumerLock.unlock();
    }
  }
}