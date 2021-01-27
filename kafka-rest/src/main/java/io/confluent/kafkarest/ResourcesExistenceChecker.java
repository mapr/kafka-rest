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

import com.mapr.streams.Streams;
import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.Partition;
import io.confluent.kafkarest.entities.PartitionReplica;
import io.confluent.kafkarest.entities.Topic;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class ResourcesExistenceChecker {

  private Admin adminClient;
  private boolean isDefaultStreamSet;
  private String defaultStream;
  private int initTimeOut;
  private String clusterId;

  public ResourcesExistenceChecker(
      KafkaRestConfig config,
      Admin adminClient
  ) {
    this.adminClient = adminClient;
    this.isDefaultStreamSet = config.isDefaultStreamSet();
    this.defaultStream = config.getString(KafkaRestConfig.STREAMS_DEFAULT_STREAM_CONFIG);
    this.initTimeOut = config.getInt(KafkaRestConfig.KAFKACLIENT_INIT_TIMEOUT_CONFIG);
    this.clusterId = getLocalCluster();
  }

  private String getLocalCluster() {
    DescribeClusterResult describeClusterResult =
            adminClient.describeCluster(
                    new DescribeClusterOptions().includeAuthorizedOperations(false));
    try {
      return describeClusterResult.clusterId().get(initTimeOut, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw Errors.kafkaErrorException(e);
    }
  }

  public boolean partitionExists(String topicName, int partition) {
    Topic topic = describeTopic(topicName);
    return (partition >= 0 && partition < topic.getPartitions().size());
  }

  public boolean topicExists(String topic) {
    Collection<String> allTopics;
    if (!isDefaultStreamSet && isStreamTopic(topic)) {
      String streamName = topic.substring(0,topic.indexOf(":"));
      String topicName = topic.substring(topic.indexOf(":") + 1);
      allTopics = getTopicNames(streamName);
      return allTopics.contains(topicName);
    } else {
      allTopics = getTopicNames();
    }
    return allTopics.contains(topic);
  }

  public boolean streamExistsFromTopicName(String topic) {
    Configuration conf = new Configuration();
    String stream;
    if (isStreamTopic(topic)) {
      stream = topic.substring(0,topic.indexOf(":"));
    } else if (isDefaultStreamSet) {
      stream = defaultStream;
    } else {
      throw new IllegalStateException("There is no stream in topic and no default stream was set");
    }
    try (com.mapr.streams.Admin admin = Streams.newAdmin(conf)) {
      return admin.streamExists(stream);
    } catch (IOException e) {
      throw Errors.kafkaErrorException(e);
    }
  }

  private boolean isStreamTopic(String topicName) {
    return topicName.startsWith("/") && topicName.contains(":");
  }

  private Collection<String> getTopicNames() {
    return getTopicNames(null);
  }

  private Collection<String> getTopicNames(String streamName) {
    Collection<String> allTopics;
    try {
      if (streamName != null) {
        allTopics = adminClient.listTopics(streamName)
                .names().get(initTimeOut, TimeUnit.MILLISECONDS);
      } else {
        allTopics = adminClient.listTopics().names().get(initTimeOut, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw Errors.kafkaErrorException(e);
    }
    return allTopics;
  }

  private Topic describeTopic(String topicName) {
    List<Topic> topicNames;
    try {
      topicNames = KafkaFutures.toCompletableFuture(adminClient
              .describeTopics(singletonList(topicName)).all())
          .thenApply(
              topics ->
                  topics.values().stream()
                      .map(topicDescription -> toTopic(clusterId, topicDescription))
                      .collect(Collectors.toList()))
          .get(initTimeOut, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw Errors.kafkaErrorException(e);
    }
    if (topicNames == null || topicNames.isEmpty()) {
      Errors.topicNotFoundException();
    }
    if (topicNames.size() > 1) {
      throw new IllegalStateException(
          String.format("More than one topic exists with name %s.", topicName));
    }
    return topicNames.get(0);
  }

  private static Topic toTopic(String clusterId, TopicDescription topicDescription) {
    return Topic.create(
        clusterId,
        topicDescription.name(),
        topicDescription.partitions().stream()
            .map(partition -> toPartition(clusterId, topicDescription.name(), partition))
            .collect(Collectors.toList()),
        (short) topicDescription.partitions().get(0).replicas().size(),
        topicDescription.isInternal());
  }

  private static Partition toPartition(
      String clusterId, String topicName, TopicPartitionInfo partitionInfo) {
    Set<Node> inSyncReplicas = new HashSet<>(partitionInfo.isr());
    List<PartitionReplica> replicas = new ArrayList<>();
    for (Node replica : partitionInfo.replicas()) {
      replicas.add(
          PartitionReplica.create(
              clusterId,
              topicName,
              partitionInfo.partition(),
              replica.id(),
              partitionInfo.leader().equals(replica),
              inSyncReplicas.contains(replica)));
    }
    return Partition.create(clusterId, topicName, partitionInfo.partition(), replicas);
  }

  private void checkStreamExistsFromTopicName(final String topic) {
    if (!streamExistsFromTopicName(topic)) {
      throw Errors.streamNotFoundException();
    }
  }

  private void checkTopicExists(final String topic) {
    if (!topicExists(topic)) {
      throw Errors.topicNotFoundException();
    }
  }

  private void checkPartitionExists(final String topic, int partition) {
    if (!partitionExists(topic, partition)) {
      throw Errors.partitionNotFoundException();
    }
  }

  public void checkIfTopicAndPartitionExists(final String topic, int partition) {
    checkStreamExistsFromTopicName(topic);
    checkTopicExists(topic);
    checkPartitionExists(topic, partition);
  }
}
