// Copyright 2021 Sergey Savelev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.Common.Contract;
using SergeSavel.KafkaRestProxy.Common.Exceptions;
using SergeSavel.KafkaRestProxy.Common.Mappers;
using SergeSavel.KafkaRestProxy.Consumer.Responses;
using KafkaException = Confluent.Kafka.KafkaException;
using TopicMetadata = SergeSavel.KafkaRestProxy.Common.Responses.TopicMetadata;
using TopicPartition = SergeSavel.KafkaRestProxy.Consumer.Responses.TopicPartition;
using TopicPartitionOffset = SergeSavel.KafkaRestProxy.Consumer.Requests.TopicPartitionOffset;

namespace SergeSavel.KafkaRestProxy.Consumer
{
    public class ConsumerWrapper : ClientWrapper
    {
        private readonly IConsumer<string, string> _consumer;
        private readonly ISchemaRegistryClient _schemaRegistryClient;

        private readonly SemaphoreSlim _semaphore = new(1);
        private IDeserializer<string> _avroDeserializer;
        private IAdminClient _dependentAdminClient;

        public ConsumerWrapper(string name, IDictionary<string, string> config, KeyValueType keyType,
            KeyValueType valueType, ISchemaRegistryClient schemaRegistryClient, TimeSpan expirationTimeout) : base(name,
            config, expirationTimeout)
        {
            _schemaRegistryClient = schemaRegistryClient;
            KeyType = keyType;
            ValueType = valueType;
            var keyDeserializer = GetDeserializer(keyType);
            var valueDeserializer = GetDeserializer(valueType);
            _consumer = new ConsumerBuilder<string, string>(config)
                .SetKeyDeserializer(keyDeserializer)
                .SetValueDeserializer(valueDeserializer)
                .Build();
        }

        public KeyValueType KeyType { get; }

        public KeyValueType ValueType { get; }

        public ICollection<TopicPartition> GetAssignment()
        {
            return _consumer.Assignment
                .Select(Map)
                .ToList();
        }

        public void Assign(IEnumerable<TopicPartitionOffset> assignment)
        {
            _consumer.Assign(assignment.Select(Map));
        }

        public ConsumerMessage Consume(CancellationToken cancellationToken = default)
        {
            ConsumeResult<string, string> consumeResult;
            try
            {
                consumeResult = _consumer.Consume(cancellationToken);
            }
            catch (ConsumeException e)
            {
                throw new Exceptions.ConsumeException("Unable to receive message.", e);
            }

            return Map(consumeResult);
        }

        public ConsumerMessage Consume(TimeSpan timeout)
        {
            ConsumeResult<string, string> consumeResult;
            try
            {
                consumeResult = _consumer.Consume(timeout);
            }
            catch (ConsumeException e)
            {
                throw new Exceptions.ConsumeException("Unable to receive message.", e);
            }

            return Map(consumeResult);
        }

        public PartitionOffsets GetWatermarkOffsets(string topic, int partition)
        {
            var topicPartition = new Confluent.Kafka.TopicPartition(topic, partition);
            WatermarkOffsets watermarkOffsets;
            try
            {
                watermarkOffsets = _consumer.GetWatermarkOffsets(topicPartition);
            }
            catch (KafkaException e)
            {
                throw new Exceptions.ConsumeException("Unable to get partition offsets.", e);
            }

            var position = _consumer.Position(topicPartition);
            return new PartitionOffsets
            {
                Topic = topicPartition.Topic,
                Partition = topicPartition.Partition,
                High = watermarkOffsets.High,
                Low = watermarkOffsets.Low,
                Current = Map(position)
            };
        }

        public PartitionOffsets QueryWatermarkOffsets(string topic, int partition, TimeSpan timeout)
        {
            var topicPartition = new Confluent.Kafka.TopicPartition(topic, partition);
            WatermarkOffsets watermarkOffsets;
            try
            {
                watermarkOffsets = _consumer.QueryWatermarkOffsets(topicPartition, timeout);
            }
            catch (KafkaException e)
            {
                throw new Exceptions.ConsumeException("Unable to query partition offsets.", e);
            }

            var position = _consumer.Position(topicPartition);
            return new PartitionOffsets
            {
                Topic = topicPartition.Topic,
                Partition = topicPartition.Partition,
                High = watermarkOffsets.High,
                Low = watermarkOffsets.Low,
                Current = Map(position)
            };
        }

        public TopicMetadata GetTopicMetadata(string topic, TimeSpan timeout)
        {
            var adminClient = GetAdminClient();
            var metadata = adminClient.GetMetadata(topic, timeout);
            var topicMetadata = metadata.Topics[0];
            if (topicMetadata.Error.Code == ErrorCode.UnknownTopicOrPart)
                throw new TopicNotFoundException(topic);
            return CommonMapper.Map(topicMetadata, metadata);
        }

        private IDeserializer<string> GetDeserializer(KeyValueType keyValueType)
        {
            return keyValueType switch
            {
                KeyValueType.Null => StringDeserializers.Null,
                KeyValueType.Ignore => StringDeserializers.Ignore,
                KeyValueType.Bytes => StringDeserializers.Base64,
                KeyValueType.String => StringDeserializers.Utf8,
                KeyValueType.AvroAsXml => GetAvroDeserializer(),
                _ => throw new ArgumentException("Unexpected key/value type: ")
            };
        }

        private IDeserializer<string> GetAvroDeserializer()
        {
            if (_avroDeserializer == null)
            {
                _semaphore.Wait(TimeSpan.FromSeconds(10));
                try
                {
                    if (_schemaRegistryClient == null)
                        throw new DeserializationException("SchemaRegistry Client not initialized.");

                    _avroDeserializer = new StringAvroDeserializer(_schemaRegistryClient);
                }
                finally
                {
                    _semaphore.Release();
                }
            }

            return _avroDeserializer;
        }

        private IAdminClient GetAdminClient()
        {
            if (_dependentAdminClient == null)
            {
                _semaphore.Wait(TimeSpan.FromSeconds(10));
                try
                {
                    _dependentAdminClient = new DependentAdminClientBuilder(_consumer.Handle).Build();
                }
                finally
                {
                    _semaphore.Release();
                }
            }

            return _dependentAdminClient;
        }

        private static TopicPartition Map(Confluent.Kafka.TopicPartition source)
        {
            return new TopicPartition
            {
                Topic = source.Topic,
                Partition = source.Partition
            };
        }

        private static Confluent.Kafka.TopicPartitionOffset Map(TopicPartitionOffset source)
        {
            return new Confluent.Kafka.TopicPartitionOffset(source.Topic, source.Partition, source.Offset);
        }

        private static ConsumerMessage Map(ConsumeResult<string, string> source)
        {
            if (source == null) return null;
            return new ConsumerMessage
            {
                Topic = source.Topic,
                Partition = source.Partition,
                Offset = source.Offset,
                Timestamp = source.Message?.Timestamp.UnixTimestampMs,
                Key = source.Message?.Key,
                Headers = source.Message?.Headers
                    .ToDictionary(h => h.Key, h => Map(h.GetValueBytes())),
                Value = source.Message?.Value,
                IsPartitionEOF = source.IsPartitionEOF
            };
        }

        private static string Map(byte[] bytes)
        {
            if (bytes == null) return null;
            return Encoding.UTF8.GetString(bytes);
        }

        private static PartitionOffsets.Offset Map(Offset offset)
        {
            string specialValue = null;

            if (offset.IsSpecial)
                specialValue = offset.Value == Offset.Beginning.Value ? "Beginning"
                    : offset.Value == Offset.End.Value ? "End"
                    : offset.Value == Offset.Stored.Value ? "Stored"
                    : offset.Value == Offset.Unset.Value ? "Unset"
                    : throw new ArgumentOutOfRangeException(nameof(offset));

            return new PartitionOffsets.Offset
            {
                Value = offset.Value,
                IsSpecial = offset.IsSpecial,
                SpecialValue = specialValue
            };
        }

        public override void Dispose()
        {
            try
            {
                _consumer.Close();
            }
            catch (Exception)
            {
                // ignore
            }

            try
            {
                _dependentAdminClient?.Dispose();
            }
            catch (Exception)
            {
                // ignore
            }

            _consumer.Dispose();
        }
    }
}