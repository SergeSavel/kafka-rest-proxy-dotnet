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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.Common.Contract;
using SergeSavel.KafkaRestProxy.Common.Extensions;
using SergeSavel.KafkaRestProxy.Producer.Contract;
using SergeSavel.KafkaRestProxy.Producer.Exceptions;
using SergeSavel.KafkaRestProxy.Producer.Responses;

namespace SergeSavel.KafkaRestProxy.Producer
{
    public class ProducerWrapper : ClientWrapper
    {
        private static readonly ISerializer<byte[]> XSerializer = new RawSerializer();
        private readonly IProducer<byte[], byte[]> _producer;
        private readonly ConcurrentDictionary<string, RecordSchema> _schemaCache = new();

        private readonly ISchemaRegistryClient _schemaRegistryClient;

        private readonly SemaphoreSlim _semaphore = new(1);
        private AvroSerializer<GenericRecord> _avroSerializer;

        public ProducerWrapper(string name, IEnumerable<KeyValuePair<string, string>> config,
            ISchemaRegistryClient schemaRegistryClient, TimeSpan expirationTimeout) : base(name, expirationTimeout)
        {
            _schemaRegistryClient = schemaRegistryClient;
            _producer = new ProducerBuilder<byte[], byte[]>(config)
                .SetKeySerializer(XSerializer)
                .SetValueSerializer(XSerializer)
                .Build();
        }

        public async Task<DeliveryResult> ProduceAsync(string topic, int? partition, IMessage message)
        {
            var kafkaHeaders = Map(message.Headers);

            var keyContext = new SerializationContext(MessageComponentType.Key, topic, kafkaHeaders);
            var keyBytes = await SerializeAsync(keyContext, message.Key, message.KeyType, message.KeySchema)
                .ConfigureAwait(false);

            var valueContext = new SerializationContext(MessageComponentType.Value, topic, kafkaHeaders);
            var valueBytes = await SerializeAsync(valueContext, message.Value, message.ValueType, message.ValueSchema)
                .ConfigureAwait(false);

            var kafkaMessage = new Message<byte[], byte[]>
            {
                Key = keyBytes,
                Value = valueBytes,
                Headers = kafkaHeaders
            };

            DeliveryResult<byte[], byte[]> producerDeliveryResult;
            try
            {
                producerDeliveryResult = partition.HasValue
                    ? await _producer.ProduceAsync(new TopicPartition(topic, partition.Value), kafkaMessage)
                        .ConfigureAwait(false)
                    : await _producer.ProduceAsync(topic, kafkaMessage)
                        .ConfigureAwait(false);
            }
            catch (KafkaException e)
            {
                throw new ProduceException(e);
            }

            var deliveryResult = Map(producerDeliveryResult);

            return deliveryResult;
        }

        private async Task<byte[]> SerializeAsync(SerializationContext serializationContext, string stringData,
            KeyValueType dataType, string schema)
        {
            byte[] result;
            switch (dataType)
            {
                case KeyValueType.Null:
                    result = Serializers.Null.Serialize(null, serializationContext);
                    break;
                case KeyValueType.String:
                    result = Serializers.Utf8.Serialize(stringData, serializationContext);
                    break;
                case KeyValueType.Bytes:
                {
                    var data = Convert.FromBase64String(stringData);
                    result = Serializers.ByteArray.Serialize(data, serializationContext);
                    break;
                }
                case KeyValueType.AvroAsXml:
                {
                    GenericRecord genericRecord;
                    try
                    {
                        genericRecord = stringData.AsGenericRecord(schema, _schemaCache);
                    }
                    catch (Exception e)
                    {
                        throw new SerializationException("An error occured while parsing generic record.", e);
                    }

                    result = await GetAvroSerializer().SerializeAsync(genericRecord, serializationContext);
                    break;
                }
                default:
                    throw new ArgumentException("Unexpected data serialization type: " + dataType, nameof(dataType));
            }

            return result;
        }

        private AvroSerializer<GenericRecord> GetAvroSerializer()
        {
            if (_avroSerializer == null)
            {
                _semaphore.Wait(TimeSpan.FromSeconds(10));
                try
                {
                    if (_schemaRegistryClient == null)
                        throw new SerializationException("SchemaRegistry Client not initialized.");

                    _avroSerializer = new AvroSerializer<GenericRecord>(_schemaRegistryClient);
                }
                finally
                {
                    _semaphore.Release();
                }
            }

            return _avroSerializer;
        }

        private static Headers Map(IEnumerable<KeyValuePair<string, string>> source)
        {
            var result = new Headers();

            if (source != null)
                foreach (var (key, stringValue) in source)
                    if (key != null)
                    {
                        byte[] value = null;
                        if (stringValue != null)
                            value = Encoding.UTF8.GetBytes(stringValue);
                        result.Add(key, value);
                    }

            return result;
        }

        private static DeliveryResult Map<TKey, TValue>(DeliveryResult<TKey, TValue> source)
        {
            return new DeliveryResult
            {
                Status = Enum.GetName(source.Status),
                Topic = source.Topic,
                Partition = source.Partition.Value,
                Offset = source.Offset,
                Timestamp = source.Timestamp.UnixTimestampMs
            };
        }

        public override void Dispose()
        {
            try
            {
                _producer.Flush(TimeSpan.FromSeconds(10));
            }
            catch (Exception e)
            {
                // ignore
            }

            _producer.Dispose();
            _semaphore.Dispose();
        }

        private class RawSerializer : ISerializer<byte[]>
        {
            public byte[] Serialize(byte[] data, SerializationContext context)
            {
                return data;
            }
        }
    }
}