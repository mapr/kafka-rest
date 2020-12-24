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

import io.confluent.kafkarest.util.NotAllowedMethodFilter;
import org.eclipse.jetty.util.StringUtil;
import org.apache.kafka.common.security.JaasUtils;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Configurable;

import io.confluent.kafkarest.exceptions.ZkExceptionMapper;
import io.confluent.kafkarest.extension.ContextInvocationHandler;
import io.confluent.kafkarest.extension.KafkaRestCleanupFilter;
import io.confluent.kafkarest.extension.KafkaRestContextProvider;
import io.confluent.kafkarest.extension.RestResourceExtension;
import io.confluent.kafkarest.extension.SchemaRegistryEnabledRequestFilter;
import io.confluent.kafkarest.resources.BrokersResource;
import io.confluent.kafkarest.resources.ConsumersResource;
import io.confluent.kafkarest.resources.PartitionsResource;
import io.confluent.kafkarest.resources.RootResource;
import io.confluent.kafkarest.resources.TopicsResource;
import io.confluent.kafkarest.resources.StreamsResource;
import io.confluent.kafkarest.v2.KafkaConsumerManager;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfigException;
import kafka.utils.ZkUtils;


/**
 * Utilities for configuring and running an embedded Kafka server.
 */
public class KafkaRestApplication extends Application<KafkaRestConfig> {

  List<RestResourceExtension> restResourceExtensions;
  boolean isStreams;
  KafkaRestContext context;

  public KafkaRestApplication() throws RestConfigException {
    this(new Properties());
  }

  public KafkaRestApplication(Properties props) throws RestConfigException {
    super(new KafkaRestConfig(props));
  }

  public KafkaRestApplication(KafkaRestConfig config)
      throws IllegalAccessException, InstantiationException, RestConfigException {
    super(config);

    restResourceExtensions = config.getConfiguredInstances(
        KafkaRestConfig.KAFKA_REST_RESOURCE_EXTENSION_CONFIG,
        RestResourceExtension.class);
  }


  @Override
  public void setupResources(Configurable<?> config, KafkaRestConfig appConfig) {
    setupInjectedResources(config, appConfig, null, null, null, null, null, null, null,
        null
    );
  }

  /**
   * Helper that does normal setup, but uses injected components so their configs or implementations
   * can be customized for testing. This only exists to support TestKafkaRestApplication
   */
  protected void setupInjectedResources(
      Configurable<?> config, KafkaRestConfig appConfig,
      ZkUtils zkUtils, KafkaStreamsMetadataObserver mdObserver,
      ProducerPool producerPool,
      ConsumerManager consumerManager,
      SimpleConsumerFactory simpleConsumerFactory,
      SimpleConsumerManager simpleConsumerManager,
      KafkaConsumerManager kafkaConsumerManager,
      AdminClientWrapper adminClientWrapperInjected
  ) {

    isStreams = appConfig.isStreams();
    if ((appConfig.isImpersonationEnabled()) &&
                          ! appConfig.getString(KafkaRestConfig.AUTHENTICATION_REALM_CONFIG).equals("jpamLogin")){
               throw new RuntimeException("PAM Authentication must be enabled in order to support MapR Streams impersonation");
                }
      
    if (!isStreams){
        if (zkUtils == null
                && StringUtil.isNotBlank(appConfig.getString(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG))) {

            zkUtils = ZkUtils.apply(
                    appConfig.getString(KafkaRestConfig.ZOOKEEPER_CONNECT_CONFIG), 30000,
                    30000,
                    JaasUtils.isZkSecurityEnabled()
            );
        }
    }
    mdObserver = new KafkaStreamsMetadataObserver(appConfig, zkUtils, appConfig.isStreams(), appConfig.isImpersonationEnabled());
   
    if (producerPool == null) {
     producerPool = new ProducerPool(appConfig, null);
    }
      
    if (consumerManager == null) {
      consumerManager = new ConsumerManager(appConfig, mdObserver);
    }
    if (simpleConsumerFactory == null) {
      simpleConsumerFactory = new SimpleConsumerFactory(appConfig);
    }
    if (simpleConsumerManager == null) {
      simpleConsumerManager =
          new SimpleConsumerManager(appConfig, mdObserver, simpleConsumerFactory);
    }
    context =
        new DefaultKafkaRestContext(appConfig, mdObserver, producerPool, consumerManager,
            simpleConsumerManager, kafkaConsumerManager, adminClientWrapperInjected, zkUtils,
            appConfig.isStreams(), appConfig.isImpersonationEnabled());
    
    config.register(RootResource.class);
    config.register(new BrokersResource(context));
    config.register(new TopicsResource(context));
    config.register(new PartitionsResource(context));
    config.register(new ConsumersResource(context));
    config.register(new StreamsResource(context));
    config.register(new io.confluent.kafkarest.resources.v2.ConsumersResource(context));
    config.register(new io.confluent.kafkarest.resources.v2.PartitionsResource(context));
    config.register(KafkaRestCleanupFilter.class);
    config.register(new SchemaRegistryEnabledRequestFilter(context));

    disableNotAllowedHttpMethods(config);

    for (RestResourceExtension restResourceExtension : restResourceExtensions) {
      restResourceExtension.register(config, appConfig);
    }
  }

  public static void disableNotAllowedHttpMethods(Configurable<?> config) {
    ContainerRequestFilter filter = new NotAllowedMethodFilter();
    config.register(filter);
  }

  @Override
  public void onShutdown() {

    for (RestResourceExtension restResourceExtension : restResourceExtensions) {
      restResourceExtension.clean();
    }

    context.getConsumerManager().shutdown();
    context.getProducerPool().shutdown();
    context.getSimpleConsumerManager().shutdown();
    context.getMetadataObserver().shutdown();
  }
}
