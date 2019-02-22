/**
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

import org.apache.kafka.clients.consumer.Consumer;

/**
 * Wraps state of assigned Consumer to a single topic partition.
 */
public class TPConsumerState implements AutoCloseable {

    private Consumer<byte[], byte[]> consumer;
    private SimpleConsumerPool ownerPool;
    private String clientId;
    private final boolean isStreams;

    public TPConsumerState(Consumer<byte[], byte[]> consumer,
                           final boolean isStreams,
                           SimpleConsumerPool ownerPool,
                           String clientId) {
        this.consumer = consumer;
        this.isStreams = isStreams;
        this.ownerPool = ownerPool;
        this.clientId = clientId;
    }

    public String clientId() {
        return clientId;
    }

    public Consumer<byte[], byte[]> consumer() {
        return consumer;
    }

    public void close() throws Exception {
        // release partition
        consumer.unsubscribe();
        ownerPool.release(this);
    }

    public boolean isStreams() {
        return isStreams;
    }
}