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

package io.confluent.kafkarest.controllers;

import static io.confluent.kafkarest.controllers.Entities.checkEntityExists;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.common.KafkaFutures;
import io.confluent.kafkarest.entities.ConsumerGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.ConsumerGroupState;

final class ConsumerGroupManagerImpl implements ConsumerGroupManager {

  private final Admin adminClient;
  private final ClusterManager clusterManager;

  @Inject
  ConsumerGroupManagerImpl(Admin adminClient, ClusterManager clusterManager) {
    this.adminClient = requireNonNull(adminClient);
    this.clusterManager = requireNonNull(clusterManager);
  }

  @Override
  public CompletableFuture<List<ConsumerGroup>> listConsumerGroups(String clusterId) {
    return clusterManager
        .getCluster(clusterId)
        .thenApply(
            cluster -> checkEntityExists(cluster, "Cluster %s could not be found.", clusterId))
        .thenCompose(
            cluster -> KafkaFutures.toCompletableFuture(adminClient.listConsumerGroups().all()))
        .thenCompose(
            listings ->
                getConsumerGroups(
                    clusterId,
                    listings.stream()
                        .map(ConsumerGroupListing::groupId)
                        .collect(Collectors.toList())));
  }

  @Override
  public CompletableFuture<Optional<ConsumerGroup>> getConsumerGroup(
      String clusterId, String consumerGroupId) {
    return clusterManager
        .getCluster(clusterId)
        .thenApply(
            cluster -> checkEntityExists(cluster, "Cluster %s could not be found.", clusterId))
        .thenCompose(cluster -> getConsumerGroups(clusterId, singletonList(consumerGroupId)))
        .thenApply(consumerGroups -> consumerGroups.stream().findAny());
  }

  private CompletableFuture<List<ConsumerGroup>> getConsumerGroups(
      String clusterId, List<String> consumerGroupIds) {
    return KafkaFutures.toCompletableFuture(
            adminClient.describeConsumerGroups(consumerGroupIds).all())
        .thenApply(
            descriptions -> {
              List<ConsumerGroupState> states =
                  descriptions.values().stream()
                      .map(description -> description.state())
                      .collect(Collectors.toList());
              List<String> assignors =
                  descriptions.values().stream()
                      .map(description -> description.partitionAssignor())
                      .collect(Collectors.toList());
              for (ConsumerGroupState state : states) {
                if (state == ConsumerGroupState.UNKNOWN) {
                  throw new IllegalStateException("before getConsumerGroups - States: " + states);
                }
              }
              for (String assignor : assignors) {
                if (assignor == null || assignor.equals("")) {
                  throw new IllegalStateException(
                      "before getConsumerGroups - Assignors: " + assignors);
                }
              }

              List<ConsumerGroup> consumerGroups =
                  descriptions.values().stream()
                      .filter(
                          // When describing a consumer-group that does not exist, AdminClient
                          // returns
                          // a dummy consumer-group with simple=true and state=DEAD.
                          // TODO: Investigate a better way of detecting non-existent
                          // consumer-group.
                          description ->
                              !description.isSimpleConsumerGroup()
                                  || description.state() != ConsumerGroupState.DEAD)
                      .map(
                          description ->
                              ConsumerGroup.fromConsumerGroupDescription(clusterId, description))
                      .collect(Collectors.toList());

              List<ConsumerGroup.State> statesAfter = new ArrayList<ConsumerGroup.State>();
              List<String> assignorsAfter = new ArrayList<String>();
              for (ConsumerGroup group : consumerGroups) {
                statesAfter.add(group.getState());
                assignorsAfter.add(group.getPartitionAssignor());
              }

              throw new IllegalStateException(
                  "after getConsumerGroups - States: "
                      + statesAfter
                      + ", Assignors: "
                      + assignorsAfter);

              return consumerGroups;
            });
  }
}
