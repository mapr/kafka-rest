/*
 * Copyright 2015 Confluent Inc.
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

import io.confluent.kafkarest.v2.KafkaConsumerManager;
import kafka.utils.ZkUtils;

/**
 * Shared, global state for the REST proxy server, including configuration and connection pools.
 * ProducerPool, AdminClientWrapper and KafkaConsumerManager instances are initialized lazily
 * if required.
 */
public class DefaultKafkaRestContext implements KafkaRestContext {

    private final KafkaRestConfig config;
    private final KafkaStreamsMetadataObserver metadataObserver;
    private ProducerPool producerPool;
    private final ConsumerManager consumerManager;
    private KafkaConsumerManager kafkaConsumerManager;
    private final SimpleConsumerManager simpleConsumerManager;
    private AdminClientWrapper adminClientWrapper;
    private final ZkUtils zkUtils;
    private final boolean isStreams;
    private final boolean isImpersonationEnabled;


    public DefaultKafkaRestContext(
            KafkaRestConfig config,
            KafkaStreamsMetadataObserver metadataObserver,
            ProducerPool producerPool,
            ConsumerManager consumerManager,
            SimpleConsumerManager simpleConsumerManager,
            KafkaConsumerManager kafkaConsumerManager,
            AdminClientWrapper adminClientWrapper,
            ZkUtils zkUtils,
            boolean isStreams,
            boolean isImpersonationEnabled
    ) {

        this.config = config;
        this.metadataObserver = metadataObserver;
        this.producerPool = producerPool;
        this.consumerManager = consumerManager;
        this.simpleConsumerManager = simpleConsumerManager;
        this.kafkaConsumerManager = kafkaConsumerManager;
        this.adminClientWrapper = adminClientWrapper;
        this.zkUtils = zkUtils;
        this.isStreams = isStreams;
        this.isImpersonationEnabled = isImpersonationEnabled;
    }


    @Override
    public KafkaRestConfig getConfig() {
        return config;
    }

    @Override
    public KafkaStreamsMetadataObserver getMetadataObserver() {
        if (isImpersonationEnabled) {
            return new KafkaStreamsMetadataObserver(config, zkUtils, isStreams, isImpersonationEnabled);
        } else {
            return metadataObserver;
        }
    }

    @Override
    public ProducerPool getProducerPool() {
        if (isImpersonationEnabled) {
            return new ProducerPool(config);
        } else {
            return producerPool;
        }
    }

    @Override
    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    @Override
    public SimpleConsumerManager getSimpleConsumerManager() {
        if (isImpersonationEnabled) {
            return new SimpleConsumerManager(config, getMetadataObserver(),
                    new SimpleConsumerFactory(config));
        } else {
            return simpleConsumerManager;
        }
    }

    @Override
    public KafkaConsumerManager getKafkaConsumerManager() {
        if (kafkaConsumerManager == null) {
            kafkaConsumerManager = new KafkaConsumerManager(config);
        }
        return kafkaConsumerManager;
    }

    @Override
    public AdminClientWrapper getAdminClientWrapper() {
        if (adminClientWrapper == null) {
            adminClientWrapper = new AdminClientWrapper(config);
        }
        return adminClientWrapper;
    }

    public ZkUtils getZkUtils() {
        return zkUtils;
    }

    public boolean isStreams() {
        return isStreams;
    }

    public boolean isImpersonationEnabled() {
        return isImpersonationEnabled;
    }

    @Override
    public void shutdown() {
        if (kafkaConsumerManager != null) {
            kafkaConsumerManager.shutdown();
        }
        if (producerPool != null) {
            producerPool.shutdown();
        }
        if (simpleConsumerManager != null) {
            simpleConsumerManager.shutdown();
        }
        if (consumerManager != null) {
            consumerManager.shutdown();
        }
        if (adminClientWrapper != null) {
            adminClientWrapper.shutdown();
        }
        if (metadataObserver != null) {
            metadataObserver.shutdown();
        }
    }
}