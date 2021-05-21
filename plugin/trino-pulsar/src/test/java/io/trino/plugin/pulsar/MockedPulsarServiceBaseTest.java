/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.trino.plugin.pulsar;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.SameThreadOrderedSafeExecutor;
import org.apache.pulsar.broker.intercept.CounterBrokerInterceptor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

public abstract class MockedPulsarServiceBaseTest
{
    protected ServiceConfiguration conf;
    protected PulsarService pulsar;
    protected PulsarAdmin admin;
    protected PulsarClient pulsarClient;
    protected URL brokerUrl;
    protected URL brokerUrlTls;
    protected URI lookupUrl;
    protected MockZooKeeper mockZooKeeper;
    protected org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.NonClosableMockBookKeeper mockBookKeeper;
    protected boolean isTcpLookup;
    protected static final String configClusterName = "test";
    private SameThreadOrderedSafeExecutor sameThreadOrderedSafeExecutor;
    private ExecutorService bkExecutor;
    protected ZooKeeperClientFactory mockZooKeeperClientFactory = new ZooKeeperClientFactory()
    {
        public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType, int zkSessionTimeoutMillis)
        {
            return CompletableFuture.completedFuture(mockZooKeeper);
        }
    };
    private final BookKeeperClientFactory mockBookKeeperClientFactory = new BookKeeperClientFactory()
    {
        public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient, Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass, Map<String, Object> properties)
        {
            return mockBookKeeper;
        }

        public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient, Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass, Map<String, Object> properties, StatsLogger statsLogger)
        {
            return mockBookKeeper;
        }

        public void close()
        {
        }
    };
    private static final Logger log = LoggerFactory.getLogger(org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.class);

    public MockedPulsarServiceBaseTest()
    {
        this.resetConfig();
    }

    protected final void resetConfig()
    {
        this.conf = getDefaultConf();
    }

    protected final void internalSetup() throws Exception
    {
        this.init();
        this.lookupUrl = new URI(this.brokerUrl.toString());
        if (this.isTcpLookup) {
            this.lookupUrl = new URI(this.pulsar.getBrokerServiceUrl());
        }

        this.pulsarClient = this.newPulsarClient(this.lookupUrl.toString(), 0);
    }

    protected final void internalSetup(ServiceConfiguration serviceConfiguration) throws Exception
    {
        this.conf = serviceConfiguration;
        this.internalSetup();
    }

    protected final void internalSetup(boolean isPreciseDispatcherFlowControl) throws Exception
    {
        this.init(isPreciseDispatcherFlowControl);
        this.lookupUrl = new URI(this.brokerUrl.toString());
        if (this.isTcpLookup) {
            this.lookupUrl = new URI(this.pulsar.getBrokerServiceUrl());
        }

        this.pulsarClient = this.newPulsarClient(this.lookupUrl.toString(), 0);
    }

    protected PulsarClient newPulsarClient(String url, int intervalInSecs) throws PulsarClientException
    {
        return PulsarClient.builder().serviceUrl(url).statsInterval((long) intervalInSecs, TimeUnit.SECONDS).build();
    }

    protected final void internalSetupForStatsTest() throws Exception
    {
        this.init();
        String lookupUrl = this.brokerUrl.toString();
        if (this.isTcpLookup) {
            lookupUrl = (new URI(this.pulsar.getBrokerServiceUrl())).toString();
        }

        this.pulsarClient = this.newPulsarClient(lookupUrl, 1);
    }

    protected void doInitConf() throws Exception
    {
        this.conf.setBrokerServicePort(Optional.of(0));
        this.conf.setBrokerServicePortTls(Optional.of(0));
        this.conf.setAdvertisedAddress("localhost");
        this.conf.setWebServicePort(Optional.of(0));
        this.conf.setWebServicePortTls(Optional.of(0));
        this.conf.setNumExecutorThreadPoolSize(5);
    }

    protected final void init() throws Exception
    {
        this.doInitConf();
        this.sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
        this.bkExecutor = Executors.newSingleThreadExecutor((new ThreadFactoryBuilder()).setNameFormat("mock-pulsar-bk").setUncaughtExceptionHandler((thread, ex) -> {
            log.info("Uncaught exception", ex);
        }).build());
        this.mockZooKeeper = createMockZooKeeper();
        this.mockBookKeeper = createMockBookKeeper(this.mockZooKeeper, this.bkExecutor);
        this.startBroker();
    }

    protected final void init(boolean isPreciseDispatcherFlowControl) throws Exception
    {
        this.conf.setBrokerServicePort(Optional.of(0));
        this.conf.setBrokerServicePortTls(Optional.of(0));
        this.conf.setAdvertisedAddress("localhost");
        this.conf.setWebServicePort(Optional.of(0));
        this.conf.setWebServicePortTls(Optional.of(0));
        this.conf.setPreciseDispatcherFlowControl(isPreciseDispatcherFlowControl);
        this.conf.setNumExecutorThreadPoolSize(5);
        this.sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
        this.bkExecutor = Executors.newSingleThreadExecutor((new ThreadFactoryBuilder()).setNameFormat("mock-pulsar-bk").setUncaughtExceptionHandler((thread, ex) -> {
            log.info("Uncaught exception", ex);
        }).build());
        this.mockZooKeeper = createMockZooKeeper();
        this.mockBookKeeper = createMockBookKeeper(this.mockZooKeeper, this.bkExecutor);
        this.startBroker();
    }

    protected final void internalCleanup() throws Exception
    {
        if (this.admin != null) {
            this.admin.close();
            this.admin = null;
        }

        if (this.pulsarClient != null) {
            this.pulsarClient.shutdown();
            this.pulsarClient = null;
        }

        if (this.pulsar != null) {
            this.pulsar.close();
            this.pulsar = null;
        }

        if (this.mockBookKeeper != null) {
            this.mockBookKeeper.reallyShutdown();
            this.mockBookKeeper = null;
        }

        if (this.mockZooKeeper != null) {
            this.mockZooKeeper.shutdown();
            this.mockZooKeeper = null;
        }

        if (this.sameThreadOrderedSafeExecutor != null) {
            try {
                this.sameThreadOrderedSafeExecutor.shutdownNow();
                this.sameThreadOrderedSafeExecutor.awaitTermination(5L, TimeUnit.SECONDS);
            }
            catch (InterruptedException var3) {
                log.error("sameThreadOrderedSafeExecutor shutdown had error", var3);
                Thread.currentThread().interrupt();
            }

            this.sameThreadOrderedSafeExecutor = null;
        }

        if (this.bkExecutor != null) {
            try {
                this.bkExecutor.shutdownNow();
                this.bkExecutor.awaitTermination(5L, TimeUnit.SECONDS);
            }
            catch (InterruptedException var2) {
                log.error("bkExecutor shutdown had error", var2);
                Thread.currentThread().interrupt();
            }

            this.bkExecutor = null;
        }
    }

    protected abstract void setup() throws Exception;

    protected abstract void cleanup() throws Exception;

    protected void restartBroker() throws Exception
    {
        this.stopBroker();
        this.startBroker();
    }

    protected void stopBroker() throws Exception
    {
        this.pulsar.close();
        this.pulsar = null;
    }

    protected void startBroker() throws Exception
    {
        if (this.pulsar != null) {
            throw new RuntimeException("broker already started!");
        }
        else {
            this.pulsar = this.startBroker(this.conf);
            this.brokerUrl = new URL(this.pulsar.getWebServiceAddress());
            this.brokerUrlTls = new URL(this.pulsar.getWebServiceAddressTls());
            if (this.admin != null) {
                this.admin.close();
            }

            this.admin = (PulsarAdmin) Mockito.spy(PulsarAdmin.builder().serviceHttpUrl(this.brokerUrl.toString()).build());
        }
    }

    protected PulsarService startBroker(ServiceConfiguration conf) throws Exception
    {
        boolean isAuthorizationEnabled = conf.isAuthorizationEnabled();
        conf.setAuthorizationEnabled(true);
        PulsarService pulsar = this.startBrokerWithoutAuthorization(conf);
        conf.setAuthorizationEnabled(isAuthorizationEnabled);
        return pulsar;
    }

    protected PulsarService startBrokerWithoutAuthorization(ServiceConfiguration conf) throws Exception
    {
        PulsarService pulsar = (PulsarService) Mockito.spy(new PulsarService(conf));
        this.setupBrokerMocks(pulsar);
        pulsar.start();
        log.info("Pulsar started. brokerServiceUrl: {} webServiceAddress: {}", pulsar.getBrokerServiceUrl(), pulsar.getWebServiceAddress());
        return pulsar;
    }

    protected void setupBrokerMocks(PulsarService pulsar) throws Exception
    {
        ((PulsarService) Mockito.doReturn(this.mockZooKeeperClientFactory).when(pulsar)).getZooKeeperClientFactory();
        ((PulsarService) Mockito.doReturn(this.mockBookKeeperClientFactory).when(pulsar)).newBookKeeperClientFactory();
        Supplier<NamespaceService> namespaceServiceSupplier = () -> {
            return (NamespaceService) Mockito.spy(new NamespaceService(pulsar));
        };
        ((PulsarService) Mockito.doReturn(namespaceServiceSupplier).when(pulsar)).getNamespaceServiceProvider();
        ((PulsarService) Mockito.doReturn(this.sameThreadOrderedSafeExecutor).when(pulsar)).getOrderedExecutor();
        ((PulsarService) Mockito.doReturn(new CounterBrokerInterceptor()).when(pulsar)).getBrokerInterceptor();
        ((PulsarService) Mockito.doAnswer((invocation) -> {
            return Mockito.spy(invocation.callRealMethod());
        }).when(pulsar)).newCompactor();
    }

    protected void waitForZooKeeperWatchers()
    {
        try {
            Thread.sleep(3000L);
        }
        catch (InterruptedException var2) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(var2);
        }
    }

    protected TenantInfo createDefaultTenantInfo() throws PulsarAdminException
    {
        if (!this.admin.clusters().getClusters().contains("test")) {
            this.admin.clusters().createCluster("test", new ClusterData());
        }

        Set<String> allowedClusters = Sets.newHashSet();
        allowedClusters.add("test");
        return new TenantInfo(Sets.newHashSet(), allowedClusters);
    }

    protected static MockZooKeeper createMockZooKeeper() throws Exception
    {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList(0);
        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:5000", "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList, CreateMode.PERSISTENT);
        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList, CreateMode.PERSISTENT);
        return zk;
    }

    protected static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.NonClosableMockBookKeeper createMockBookKeeper(ZooKeeper zookeeper, ExecutorService executor) throws Exception
    {
        return (org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.NonClosableMockBookKeeper) Mockito.spy(new org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.NonClosableMockBookKeeper(zookeeper, executor));
    }

    protected static boolean retryStrategically(Predicate<Void> predicate, int retryCount, long intSleepTimeInMillis) throws Exception
    {
        for (int i = 0; i < retryCount; ++i) {
            if (predicate.test(null) || i == retryCount - 1) {
                return true;
            }

            Thread.sleep(intSleepTimeInMillis + intSleepTimeInMillis * (long) i);
        }

        return false;
    }

    protected static void setFieldValue(Class<?> clazz, Object classObj, String fieldName, Object fieldValue) throws Exception
    {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(classObj, fieldValue);
    }

    protected static ServiceConfiguration getDefaultConf()
    {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setAdvertisedAddress("localhost");
        configuration.setClusterName("test");
        configuration.setAdvertisedAddress("localhost");
        configuration.setManagedLedgerCacheSizeMB(8);
        configuration.setActiveConsumerFailoverDelayTimeMillis(0);
        configuration.setDefaultNumberOfNamespaceBundles(1);
        configuration.setZookeeperServers("localhost:2181");
        configuration.setConfigurationStoreServers("localhost:3181");
        configuration.setAllowAutoTopicCreationType("non-partitioned");
        configuration.setBrokerServicePort(Optional.of(0));
        configuration.setBrokerServicePortTls(Optional.of(0));
        configuration.setWebServicePort(Optional.of(0));
        configuration.setWebServicePortTls(Optional.of(0));
        configuration.setBookkeeperClientExposeStatsToPrometheus(true);
        configuration.setNumExecutorThreadPoolSize(5);
        return configuration;
    }

    public static class NonClosableMockBookKeeper
            extends PulsarMockBookKeeper
    {
        public NonClosableMockBookKeeper(ZooKeeper zk, ExecutorService executor) throws Exception
        {
            super(zk, executor);
        }

        public void close()
        {
        }

        public void shutdown()
        {
        }

        public void reallyShutdown()
        {
            super.shutdown();
        }
    }
}
