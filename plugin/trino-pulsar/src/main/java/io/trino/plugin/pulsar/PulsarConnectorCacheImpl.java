/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.pulsar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.shade.org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.shade.org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.offload.Offloaders;
import org.apache.pulsar.shade.org.apache.bookkeeper.mledger.offload.OffloadersCache;
import org.apache.pulsar.shade.org.apache.bookkeeper.stats.StatsProvider;
import org.apache.pulsar.shade.org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.shade.org.apache.pulsar.common.policies.data.OffloadPolicies;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A cache for the Pulsar connector.
 */
public class PulsarConnectorCacheImpl
        implements PulsarConnectorCache
{
    private static final Logger log = Logger.get(PulsarConnectorCacheImpl.class);

    @VisibleForTesting
    static PulsarConnectorCache instance;

    private final ManagedLedgerFactory managedLedgerFactory;

    private final StatsProvider statsProvider;
    private OrderedScheduler offloaderScheduler;
    private OffloadersCache offloadersCache = new OffloadersCache();
    private LedgerOffloader defaultOffloader;
    private Map<NamespaceName, LedgerOffloader> offloaderMap = new ConcurrentHashMap<>();

    protected PulsarConnectorCacheImpl(PulsarConnectorConfig pulsarConnectorConfig) throws Exception
    {
        managedLedgerFactory = initManagedLedgerFactory(pulsarConnectorConfig);
        statsProvider = PulsarConnectorUtils.createInstance(pulsarConnectorConfig.getStatsProvider(),
                StatsProvider.class, getClass().getClassLoader());

        // start stats provider
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        pulsarConnectorConfig.getStatsProviderConfigs().forEach(clientConfiguration::setProperty);

        statsProvider.start(clientConfiguration);

        defaultOffloader = initManagedLedgerOffloader(
                pulsarConnectorConfig.getOffloadPolices(), pulsarConnectorConfig);
    }

    public static PulsarConnectorCache getConnectorCache(PulsarConnectorConfig pulsarConnectorConfig) throws Exception
    {
        synchronized (PulsarConnectorCache.class) {
            if (instance == null) {
                instance = new PulsarConnectorCacheImpl(pulsarConnectorConfig);
            }
        }
        return instance;
    }

    private static ManagedLedgerFactory initManagedLedgerFactory(PulsarConnectorConfig pulsarConnectorConfig)
            throws Exception
    {
        ClientConfiguration bkClientConfiguration = new ClientConfiguration()
                .setMetadataServiceUri("zk://" + pulsarConnectorConfig.getZookeeperUri()
                        .replace(",", ";") + "/ledgers")
                .setClientTcpNoDelay(false)
                .setUseV2WireProtocol(pulsarConnectorConfig.getBookkeeperUseV2Protocol())
                .setExplictLacInterval(pulsarConnectorConfig.getBookkeeperExplicitInterval())
                .setStickyReadsEnabled(false)
                .setReadEntryTimeout(60)
                .setThrottleValue(pulsarConnectorConfig.getBookkeeperThrottleValue())
                .setNumIOThreads(pulsarConnectorConfig.getBookkeeperNumIOThreads())
                .setNumWorkerThreads(pulsarConnectorConfig.getBookkeeperNumWorkerThreads());

        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();
        managedLedgerFactoryConfig.setMaxCacheSize(pulsarConnectorConfig.getManagedLedgerCacheSizeMB());
        managedLedgerFactoryConfig.setNumManagedLedgerWorkerThreads(
                pulsarConnectorConfig.getManagedLedgerNumWorkerThreads());
        managedLedgerFactoryConfig.setNumManagedLedgerSchedulerThreads(
                pulsarConnectorConfig.getManagedLedgerNumSchedulerThreads());
        return new ManagedLedgerFactoryImpl(bkClientConfiguration, pulsarConnectorConfig.getZookeeperUri(), managedLedgerFactoryConfig);
    }

    public ManagedLedgerConfig getManagedLedgerConfig(NamespaceName namespaceName, OffloadPolicies offloadPolicies,
                                                      PulsarConnectorConfig pulsarConnectorConfig)
    {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        if (offloadPolicies == null) {
            managedLedgerConfig.setLedgerOffloader(defaultOffloader);
        }
        else {
            LedgerOffloader ledgerOffloader = offloaderMap.compute(namespaceName,
                    (ns, offloader) -> {
                        if (offloader != null && Objects.equals(offloader.getOffloadPolicies(), offloadPolicies)) {
                            return offloader;
                        }
                        else {
                            if (offloader != null) {
                                offloader.close();
                            }
                            return initManagedLedgerOffloader(offloadPolicies, pulsarConnectorConfig);
                        }
                    });
            managedLedgerConfig.setLedgerOffloader(ledgerOffloader);
        }
        return managedLedgerConfig;
    }

    private synchronized OrderedScheduler getOffloaderScheduler(OffloadPolicies offloadPolicies)
    {
        if (offloaderScheduler == null) {
            offloaderScheduler = OrderedScheduler.newSchedulerBuilder()
                    .numThreads(offloadPolicies.getManagedLedgerOffloadMaxThreads())
                    .name("pulsar-offloader").build();
        }
        return offloaderScheduler;
    }

    private LedgerOffloader initManagedLedgerOffloader(OffloadPolicies offloadPolicies,
                                                       PulsarConnectorConfig pulsarConnectorConfig)
    {
            if (!Strings.nullToEmpty(offloadPolicies.getManagedLedgerOffloadDriver()).trim().isEmpty()) {
                checkNotNull(offloadPolicies.getOffloadersDirectory(),
                        "Offloader driver is configured to be '%s' but no offloaders directory is configured.",
                        offloadPolicies.getManagedLedgerOffloadDriver());
                Offloaders offloaders = offloadersCache.getOrLoadOffloaders(offloadPolicies.getOffloadersDirectory(),
                        pulsarConnectorConfig.getNarExtractionDirectory());
                LedgerOffloaderFactory offloaderFactory = null;
                try {
                    offloaderFactory = offloaders.getOffloaderFactory(
                            offloadPolicies.getManagedLedgerOffloadDriver());
                } catch (IOException ioe) {
                    log.error("Failed to fetch offloaderFactory: ", ioe);
                    throw new RuntimeException(ioe.getMessage(), ioe.getCause());                }
                try {
                    return offloaderFactory.create(
                            offloadPolicies,
                            ImmutableMap.of(
                                    LedgerOffloader.METADATA_SOFTWARE_VERSION_KEY.toLowerCase(), PulsarVersion.getVersion(),
                                    LedgerOffloader.METADATA_SOFTWARE_GITSHA_KEY.toLowerCase(), PulsarVersion.getGitSha()),
                            getOffloaderScheduler(offloadPolicies));
                }
                catch (IOException ioe) {
                    log.error("Failed to create offloader: ", ioe);
                    throw new RuntimeException(ioe.getMessage(), ioe.getCause());
                }
            }
            else {
                return NullLedgerOffloader.INSTANCE;
            }

    }

    @Override
    public ManagedLedgerFactory getManagedLedgerFactory()
    {
        return managedLedgerFactory;
    }

    @Override
    public StatsProvider getStatsProvider()
    {
        return statsProvider;
    }

    public static void shutdown() throws Exception
    {
        synchronized (PulsarConnectorCacheImpl.class) {
            if (instance != null) {
                PulsarConnectorCacheImpl impl = (PulsarConnectorCacheImpl) instance;
                impl.statsProvider.stop();
                impl.managedLedgerFactory.shutdown();
                impl.offloaderScheduler.shutdown();
                impl.offloadersCache.close();
            }
        }
    }
}
