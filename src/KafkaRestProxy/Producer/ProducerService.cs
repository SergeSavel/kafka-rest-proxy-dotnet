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
using System.Threading;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using SergeSavel.KafkaRestProxy.Common.Contract;
using SergeSavel.KafkaRestProxy.Common.Exceptions;
using SergeSavel.KafkaRestProxy.Producer.Contract;
using SergeSavel.KafkaRestProxy.Producer.Exceptions;
using SergeSavel.KafkaRestProxy.Producer.Requests;
using SergeSavel.KafkaRestProxy.SchemaRegistry;
using KafkaException = Confluent.Kafka.KafkaException;

namespace SergeSavel.KafkaRestProxy.Producer
{
    public class ProducerService : IDisposable
    {
        private readonly IProducer<byte[], byte[]> _producer;
        private readonly ConcurrentDictionary<string, RecordSchema> _schemaCache = new();

        private readonly ISchemaRegistryClient _schemaRegistryClient;

        private readonly SemaphoreSlim _semaphore = new(1);

        private AvroSerializer<GenericRecord> _avroSerializer;

        public ProducerService(ProducerConfig config, SchemaRegistryService schemaRegistryService)
        {
            var rawSerializer = new RawSerializer();
            _producer = new ProducerBuilder<byte[], byte[]>(config)
                .SetKeySerializer(rawSerializer)
                .SetValueSerializer(rawSerializer)
                .Build();

            _schemaRegistryClient = schemaRegistryService.Client;
        }

        internal Handle Handle => _producer.Handle;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public async Task<DeliveryResult> PostMessage(string topic, int? partition, PostMessageRequest request)
        {
            var headers = ProducerMapper.MapHeaders(request.Headers);

            // Can be written better.
            var keySerializationContext = new SerializationContext(MessageComponentType.Key, topic, headers);
            byte[] keyBytes;
            switch (request.KeyType)
            {
                case KeyValueType.Null:
                    keyBytes = Serializers.Null.Serialize(null, keySerializationContext);
                    break;
                case KeyValueType.String:
                    keyBytes = Serializers.Utf8.Serialize(request.Key, keySerializationContext);
                    break;
                case KeyValueType.Bytes:
                {
                    var data = Convert.FromBase64String(request.Key);
                    keyBytes = Serializers.ByteArray.Serialize(data, keySerializationContext);
                    break;
                }
                case KeyValueType.AvroAsXml:
                {
                    var genericRecord =
                        ProducerGenericMapper.GetGenericRecord(request.Key, request.KeySchema, _schemaCache);
                    keyBytes = await GetAvroSerializer().SerializeAsync(genericRecord, keySerializationContext)
                        .ConfigureAwait(false);
                    break;
                }
                default:
                    throw new BadRequestException("Unexpected key serialization type: " + request.KeyType);
            }

            // Can be written better.
            var valueSerializationContext = new SerializationContext(MessageComponentType.Value, topic, headers);
            byte[] valueBytes;
            switch (request.ValueType)
            {
                case KeyValueType.Null:
                    valueBytes = Serializers.Null.Serialize(null, valueSerializationContext);
                    break;
                case KeyValueType.String:
                    valueBytes = Serializers.Utf8.Serialize(request.Value, valueSerializationContext);
                    break;
                case KeyValueType.Bytes:
                {
                    var data = Convert.FromBase64String(request.Value);
                    valueBytes = Serializers.ByteArray.Serialize(data, valueSerializationContext);
                    break;
                }
                case KeyValueType.AvroAsXml:
                {
                    var genericRecord =
                        ProducerGenericMapper.GetGenericRecord(request.Value, request.ValueSchema, _schemaCache);
                    valueBytes = await GetAvroSerializer().SerializeAsync(genericRecord, valueSerializationContext)
                        .ConfigureAwait(false);
                    break;
                }
                default:
                    throw new BadRequestException("Unexpected value serialization type: " + request.ValueType);
            }

            var message = new Message<byte[], byte[]>
            {
                Key = keyBytes,
                Value = valueBytes,
                Headers = headers
            };

            DeliveryResult<byte[], byte[]> producerDeliveryResult;
            try
            {
                producerDeliveryResult = partition.HasValue
                    ? await _producer.ProduceAsync(new TopicPartition(topic, partition.Value), message)
                        .ConfigureAwait(false)
                    : await _producer.ProduceAsync(topic, message).ConfigureAwait(false);
            }
            catch (KafkaException e)
            {
                throw new ProduceException(e);
            }

            var deliveryResult = ProducerMapper.Map(producerDeliveryResult);

            return deliveryResult;
        }

        private AvroSerializer<GenericRecord> GetAvroSerializer()
        {
            if (_avroSerializer == null)
            {
                _semaphore.Wait(TimeSpan.FromSeconds(10));
                try
                {
                    if (_schemaRegistryClient == null)
                        throw new BadRequestException("SchemaRegistry Client not initialized.");

                    _avroSerializer = new AvroSerializer<GenericRecord>(_schemaRegistryClient);
                }
                finally
                {
                    _semaphore.Release();
                }
            }

            return _avroSerializer;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _producer.Flush();
                _producer.Dispose();
            }
        }
    }
}