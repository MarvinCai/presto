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
package io.trino.plugin.pulsar.util;

//import com.fasterxml.jackson.annotation.JsonInclude;
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import io.netty.util.concurrent.FastThreadLocal;

public class ConversionUtil
{
//    private ConversionUtil()
//    { }
//
//    public static org.apache.pulsar.shade.io.netty.buffer.ByteBuf toShadedByteBuf(io.netty.buffer.ByteBuf inputBuffer)
//    {
//        return org.apache.pulsar.shade.io.netty.buffer.Unpooled.wrappedBuffer(inputBuffer.nioBuffer());
//    }
//
//    public static io.netty.buffer.ByteBuf toOriginalByteBuf(org.apache.pulsar.shade.io.netty.buffer.ByteBuf inputBuffer)
//    {
//        return io.netty.buffer.Unpooled.wrappedBuffer(inputBuffer.nioBuffer());
//    }
//
//    public static com.fasterxml.jackson.databind.JsonNode toOriginalJacksonNode(org.apache.pulsar.shade.com.fasterxml.jackson.databind.JsonNode inputNode) throws com.fasterxml.jackson.core.JsonProcessingException
//    {
//        return ObjectMapperFactory.getThreadLocal().readTree(inputNode.toString());
//    }
//
//    public static org.apache.pulsar.common.policies.data.OffloadPoliciesImpl toOriginalOffloadPoliciesImpl(io.trino.plugin.pulsar.util.OffloadPoliciesImpl policy)
//    {
//        return org.apache.pulsar.common.policies.data.OffloadPoliciesImpl.builder()
//        .fileSystemProfilePath(policy.getFileSystemProfilePath())
//        .fileSystemURI(policy.getFileSystemURI())
//        .gcsManagedLedgerOffloadBucket(policy.getGcsManagedLedgerOffloadBucket())
//        .gcsManagedLedgerOffloadMaxBlockSizeInBytes(policy.getGcsManagedLedgerOffloadMaxBlockSizeInBytes())
//        .gcsManagedLedgerOffloadReadBufferSizeInBytes(policy.getGcsManagedLedgerOffloadReadBufferSizeInBytes())
//        .gcsManagedLedgerOffloadRegion(policy.getGcsManagedLedgerOffloadRegion())
//        .gcsManagedLedgerOffloadServiceAccountKeyFile(policy.getGcsManagedLedgerOffloadServiceAccountKeyFile())
//        .managedLedgerOffloadBucket(policy.getManagedLedgerOffloadBucket())
//        .managedLedgerOffloadDeletionLagInMillis(policy.getManagedLedgerOffloadDeletionLagInMillis())
//        .managedLedgerOffloadDriver(policy.getManagedLedgerOffloadDriver())
//        .managedLedgerOffloadedReadPriority(policy.getManagedLedgerOffloadedReadPriority())
//        .managedLedgerOffloadMaxBlockSizeInBytes(policy.getManagedLedgerOffloadMaxBlockSizeInBytes())
//        .managedLedgerOffloadMaxThreads(policy.getManagedLedgerOffloadMaxThreads())
//        .managedLedgerOffloadPrefetchRounds(policy.getManagedLedgerOffloadPrefetchRounds())
//        .managedLedgerOffloadReadBufferSizeInBytes(policy.getManagedLedgerOffloadReadBufferSizeInBytes())
//        .managedLedgerOffloadRegion(policy.getManagedLedgerOffloadRegion())
//        .managedLedgerOffloadServiceEndpoint(policy.getManagedLedgerOffloadServiceEndpoint())
//        .managedLedgerOffloadThresholdInBytes(policy.getManagedLedgerOffloadThresholdInBytes())
//        .offloadersDirectory(policy.getOffloadersDirectory())
//        .s3ManagedLedgerOffloadBucket(policy.getS3ManagedLedgerOffloadBucket())
//        .s3ManagedLedgerOffloadCredentialId(policy.getS3ManagedLedgerOffloadCredentialId())
//        .s3ManagedLedgerOffloadCredentialSecret(policy.getS3ManagedLedgerOffloadCredentialSecret())
//        .s3ManagedLedgerOffloadMaxBlockSizeInBytes(policy.getS3ManagedLedgerOffloadMaxBlockSizeInBytes())
//        .s3ManagedLedgerOffloadReadBufferSizeInBytes(policy.getS3ManagedLedgerOffloadReadBufferSizeInBytes())
//        .s3ManagedLedgerOffloadRegion(policy.getS3ManagedLedgerOffloadRegion())
//        .s3ManagedLedgerOffloadRole(policy.getS3ManagedLedgerOffloadRole())
//        .s3ManagedLedgerOffloadServiceEndpoint(policy.getS3ManagedLedgerOffloadServiceEndpoint())
//        .s3ManagedLedgerOffloadRoleSessionName(policy.getS3ManagedLedgerOffloadRoleSessionName())
//        .build();
//    }
//
//    static class ObjectMapperFactory
//    {
//        public static ObjectMapper create()
//        {
//            ObjectMapper mapper = new ObjectMapper();
//            // forward compatibility for the properties may go away in the future
//            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//            mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
//            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
//            return mapper;
//        }
//
//        private static final FastThreadLocal<ObjectMapper> JSON_MAPPER = new FastThreadLocal<ObjectMapper>()
//        {
//            @Override
//            protected ObjectMapper initialValue() throws Exception
//            {
//                return create();
//            }
//        };
//
//        public static ObjectMapper getThreadLocal()
//        {
//            return JSON_MAPPER.get();
//        }
//    }
}
