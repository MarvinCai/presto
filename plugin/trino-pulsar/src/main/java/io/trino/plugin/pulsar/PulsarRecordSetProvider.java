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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Record sets for Pulsar.
 */
public class PulsarRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final PulsarConnectorConfig pulsarConnectorConfig;

    private final PulsarDispatchingRowDecoderFactory decoderFactory;

    @Inject
    public PulsarRecordSetProvider(PulsarConnectorConfig pulsarConnectorConfig,
                                   PulsarDispatchingRowDecoderFactory decoderFactory)
    {
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.pulsarConnectorConfig = requireNonNull(pulsarConnectorConfig, "pulsarConnectorConfig is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                  ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "Connector split is null");
        PulsarSplit pulsarSplit = (PulsarSplit) split;

        ImmutableList.Builder<PulsarColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((PulsarColumnHandle) handle);
        }

        return new PulsarRecordSet(pulsarSplit, handles.build(), pulsarConnectorConfig, decoderFactory);
    }
}
