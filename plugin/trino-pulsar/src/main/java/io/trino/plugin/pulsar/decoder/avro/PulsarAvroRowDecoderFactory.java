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
package io.trino.plugin.pulsar.decoder.avro;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.plugin.pulsar.PulsarColumnHandle;
import io.trino.plugin.pulsar.PulsarColumnMetadata;
import io.trino.plugin.pulsar.PulsarRowDecoder;
import io.trino.plugin.pulsar.PulsarRowDecoderFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * PulsarRowDecoderFactory for {@link org.apache.pulsar.common.schema.SchemaType#AVRO}.
 */
public class PulsarAvroRowDecoderFactory
        implements PulsarRowDecoderFactory
{
    private TypeManager typeManager;

    public PulsarAvroRowDecoderFactory(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public PulsarRowDecoder createRowDecoder(TopicName topicName, SchemaInfo schemaInfo,
                                             Set<DecoderColumnHandle> columns)
    {
        return new PulsarAvroRowDecoder((GenericAvroSchema) GenericAvroSchema.of(schemaInfo), columns);
    }

    @Override
    public List<ColumnMetadata> extractColumnMetadata(TopicName topicName, SchemaInfo schemaInfo,
                                                      PulsarColumnHandle.HandleKeyValueType handleKeyValueType)
    {
        List<ColumnMetadata> columnMetadata;
        String schemaJson = new String(schemaInfo.getSchema());
        if (StringUtils.isBlank(schemaJson)) {
            throw new TrinoException(NOT_SUPPORTED, "Topic "
                    + topicName.toString() + " does not have a valid schema");
        }
        Schema schema;
        try {
            schema = GenericJsonSchema.of(schemaInfo).getAvroSchema();
        }
        catch (SchemaParseException ex) {
            throw new TrinoException(NOT_SUPPORTED, "Topic "
                    + topicName.toString() + " does not have a valid schema");
        }

        try {
            columnMetadata = schema.getFields().stream()
                    .map(field ->
                            new PulsarColumnMetadata(PulsarColumnMetadata.getColumnName(handleKeyValueType, field.name()), parseAvroTrinoType(
                                    field.name(), field.schema()), field.schema().toString(), null, false, false,
                                    handleKeyValueType, new PulsarColumnMetadata.DecoderExtraInfo(field.name(),
                                    null, null))

                    ).collect(toList());
        }
        catch (StackOverflowError e) {
            log.warn(e, "Topic "
                    + topicName.toString() + " extractColumnMetadata failed.");
            throw new TrinoException(NOT_SUPPORTED, "Topic "
                    + topicName.toString() + " schema may contains cyclic definitions.", e);
        }
        return columnMetadata;
    }

    private Type parseAvroTrinoType(String fieldname, Schema schema)
    {
        Schema.Type type = schema.getType();
        LogicalType logicalType = schema.getLogicalType();
        switch (type) {
            case STRING:
            case ENUM:
                return createUnboundedVarcharType();
            case NULL:
                throw new UnsupportedOperationException(
                        format("field '%s' NULL type code should not be reached,"
                                + "please check the schema or report the bug.", fieldname));
            case FIXED:
            case BYTES:
                //TODO: support decimal logicalType
                return VarbinaryType.VARBINARY;
            case INT:
                if (logicalType == LogicalTypes.timeMillis()) {
                    return TIME;
                }
                else if (logicalType == LogicalTypes.date()) {
                    return DATE;
                }
                return IntegerType.INTEGER;
            case LONG:
                if (logicalType == LogicalTypes.timestampMillis()) {
                    return TimestampType.TIMESTAMP;
                }
                //TODO: support timestamp_microseconds logicalType : https://github.com/trinodb/trino/issues/1284
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case ARRAY:
                return new ArrayType(parseAvroTrinoType(fieldname, schema.getElementType()));
            case MAP:
                //The key for an avro map must be string
                TypeSignature valueType = parseAvroTrinoType(fieldname, schema.getValueType()).getTypeSignature();
                return typeManager.getParameterizedType(StandardTypes.MAP,
                        ImmutableList.of(TypeSignatureParameter.typeParameter(VarcharType.VARCHAR.getTypeSignature()),
                                TypeSignatureParameter.typeParameter(valueType)));
            case RECORD:
                if (schema.getFields().size() > 0) {
                    return RowType.from(schema.getFields().stream()
                            .map(field -> new RowType.Field(Optional.of(field.name()),
                                    parseAvroTrinoType(field.name(), field.schema())))
                            .collect(toImmutableList()));
                }
                else {
                    throw new UnsupportedOperationException(format(
                            "field '%s' of record type has no fields, "
                                    + "please check schema definition. ", fieldname));
                }
            case UNION:
                for (Schema nestType : schema.getTypes()) {
                    if (nestType.getType() != Schema.Type.NULL) {
                        return parseAvroTrinoType(fieldname, nestType);
                    }
                }
                throw new UnsupportedOperationException(format(
                        "field '%s' of UNION type must contains not NULL type.", fieldname));
            default:
                throw new UnsupportedOperationException(format(
                        "Can't convert from schema type '%s' (%s) to trino type.",
                        schema.getType(), schema.getFullName()));
        }
    }

    private static final Logger log = Logger.get(PulsarAvroRowDecoderFactory.class);
}
