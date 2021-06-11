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
package io.trino.plugin.pulsar.decoder.protobufnative;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
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
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarbinaryType;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.stream.Collectors.toList;

/**
 * PulsarRowDecoderFactory for {@link org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaType#PROTOBUF_NATIVE}.
 */
public class PulsarProtobufNativeRowDecoderFactory
        implements PulsarRowDecoderFactory
{
    private TypeManager typeManager;

    public PulsarProtobufNativeRowDecoderFactory(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public PulsarRowDecoder createRowDecoder(TopicName topicName,
                                             SchemaInfo schemaInfo,
                                             Set<DecoderColumnHandle> columns)
    {
        return new PulsarProtobufNativeRowDecoder((GenericProtobufNativeSchema) GenericProtobufNativeSchema.of(schemaInfo), columns);
    }

    @Override
    public List<ColumnMetadata> extractColumnMetadata(TopicName topicName,
                                                      SchemaInfo schemaInfo,
                                                      PulsarColumnHandle.HandleKeyValueType handleKeyValueType)
    {
        List<ColumnMetadata> columnMetadata;
        String schemaJson = new String(schemaInfo.getSchema(), StandardCharsets.ISO_8859_1);
        if (Strings.nullToEmpty(schemaJson).trim().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Topic "
                    + topicName.toString() + " does not have a valid schema");
        }
        Descriptors.Descriptor schema;
        try {
            schema =
                    ((GenericProtobufNativeSchema) GenericProtobufNativeSchema.of(schemaInfo)).getProtobufNativeSchema();
        }
        catch (Exception ex) {
            throw new TrinoException(NOT_SUPPORTED, "Topic "
                    + topicName.toString() + " does not have a valid schema");
        }

        //Protobuf have not yet supported Cyclic Objects.
        columnMetadata = schema.getFields().stream()
                .map(field ->
                        new PulsarColumnMetadata(PulsarColumnMetadata.getColumnName(handleKeyValueType, field.getName()),
                                parseProtobufTrinoType(field), field.getType().toString(), null, false, false,
                                handleKeyValueType, new PulsarColumnMetadata.DecoderExtraInfo(field.getName(),
                                null, null))

                ).collect(toList());

        return columnMetadata;
    }

    private Type parseProtobufTrinoType(Descriptors.FieldDescriptor field)
    {
        //parse by proto JavaType
        Descriptors.FieldDescriptor.JavaType type = field.getJavaType();
        Type dataType;
        switch (type) {
            case BOOLEAN:
                dataType = BooleanType.BOOLEAN;
                break;
            case BYTE_STRING:
                dataType = VarbinaryType.VARBINARY;
                break;
            case DOUBLE:
                dataType = DoubleType.DOUBLE;
                break;
            case ENUM:
            case STRING:
                dataType = createUnboundedVarcharType();
                break;
            case FLOAT:
                dataType = RealType.REAL;
                break;
            case INT:
                dataType = IntegerType.INTEGER;
                break;
            case LONG:
                dataType = BigintType.BIGINT;
                break;
            case MESSAGE:
                Descriptors.Descriptor msg = field.getMessageType();
                if (field.isMapField()) {
                    //map
                    TypeSignature keyType =
                            parseProtobufTrinoType(msg.findFieldByName(PulsarProtobufNativeColumnDecoder.PROTOBUF_MAP_KEY_NAME)).getTypeSignature();
                    TypeSignature valueType =
                            parseProtobufTrinoType(msg.findFieldByName(PulsarProtobufNativeColumnDecoder.PROTOBUF_MAP_VALUE_NAME)).getTypeSignature();
                    return typeManager.getParameterizedType(StandardTypes.MAP,
                            ImmutableList.of(TypeSignatureParameter.typeParameter(keyType),
                                    TypeSignatureParameter.typeParameter(valueType)));
                }
                else {
                    //row
                    dataType = RowType.from(msg.getFields().stream()
                            .map(rowField -> new RowType.Field(Optional.of(rowField.getName()),
                                    parseProtobufTrinoType(rowField)))
                            .collect(toImmutableList()));
                }
                break;
            default:
                throw new RuntimeException("Unknown type: " + type.toString() + " for FieldDescriptor: " + field.getName());
        }
        //list
        if (field.isRepeated() && !field.isMapField()) {
            dataType = new ArrayType(dataType);
        }

        return dataType;
    }

    private static final Logger log = Logger.get(PulsarProtobufNativeRowDecoderFactory.class);
}
