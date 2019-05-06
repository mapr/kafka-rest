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

import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import kafka.cluster.Broker;
import kafka.utils.ZkUtils;

/**
 * Observes metadata about the Kafka cluster and MapR streams.
 */
public class KafkaStreamsMetadataObserver extends MetadataObserver {

  private static final Logger log = LoggerFactory.getLogger(KafkaStreamsMetadataObserver.class);

  private StreamsMetadataConsumer streamsMetadataConsumer;

  private boolean defaultStreamSet;
  private String defaultStream;


  public KafkaStreamsMetadataObserver(KafkaRestConfig config, ZkUtils zkUtils) {
    super(zkUtils);

    String bootstrapServers = config.getString(KafkaRestConfig.BOOTSTRAP_SERVERS_CONFIG);
    String defaultStream =  config.getString(KafkaRestConfig.STREAMS_DEFAULT_STREAM_CONFIG);

    this.defaultStreamSet = config.isDefaultStreamSet();
    this.defaultStream = defaultStream;
    streamsMetadataConsumer = new StreamsMetadataConsumer(bootstrapServers, defaultStream);
  }
    

  @Override
  public Broker getLeader(final String topicName, final int partitionId) {
    throw Errors.notSupportedByMapRStreams();
  }

  @Override
  public Collection<String> getTopicNames() {
    try {
      return streamsMetadataConsumer.listTopics().keySet();
    } catch (KafkaException e) {
      log.warn("listTopics() API", e);
      throw Errors.notSupportedByMapRStreams(
              "Please try to set " + KafkaRestConfig.STREAMS_DEFAULT_STREAM_CONFIG
                      + " to return topics for default stream");
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
  }

  @Override
  public boolean topicExists(String topicName) {
    if (defaultStreamSet) {
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
    return defaultStreamSet || topic.startsWith("/") && topic.contains(":");
  }

  public Topic getTopic(String topicName) {
    if (defaultStreamSet) {
      return new Topic(topicName, null, getTopicPartitions(topicName));
    } else {
      if (topicName.startsWith("/") && topicName.contains(":")) {
        return new Topic(topicName, null, getTopicPartitions(topicName));
      } else {
        return null;
      }
    }
  }

  @Override
  public List<Partition> getTopicPartitions(String topic) {
    if (defaultStreamSet) {
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
  public int getLeaderId(final String topicName, final int partitionId) {
    throw Errors.notSupportedByMapRStreams();
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

  public Partition getTopicPartition(String topic, int partition) {
    List<Partition> partitions = getTopicPartitions(topic);
    for (Partition p : partitions) {
      if (p.getPartition() == partition) {
        return p;
      }
    }
    return null;
  }
 
  public boolean partitionExists(String topicName, int partition) {
    if (defaultStreamSet) {
      return (partition >= 0 && partition < streamsMetadataConsumer.partitionsFor(topicName)
          .size());
    } else {
      if (topicName.startsWith("/") && topicName.contains(":")) {
        return (partition >= 0 && partition < streamsMetadataConsumer.partitionsFor(topicName)
            .size());
      } else {
        return false;
      }
    }
  }


  private static List<Partition> convertPartitions(List<PartitionInfo> partitionInfos) {
    List<Partition> partitions = new ArrayList<>();
    for (PartitionInfo partitionInfo : partitionInfos) {
      List<PartitionReplica> replicas = new ArrayList<>();

      // the leader replica is always in sync
      replicas.add(new PartitionReplica(partitionInfo.leader().id(), true, true));

      List<Node> inSyncReplicas = Arrays.asList(partitionInfo.inSyncReplicas());
      for (Node node: partitionInfo.replicas()) {
        replicas.add(new PartitionReplica(node.id(), false, inSyncReplicas.contains(node)));
      }
      Partition partition = new Partition(partitionInfo.partition(), partitionInfo.leader().id(), 
          replicas);
      partitions.add(partition);
    }
    return partitions;
  }
}


