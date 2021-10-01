﻿// Copyright 2021 Sergey Savelev
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
using System.Text;
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

        private readonly SemaphoreSlim _producerSemaphore = new(1);
        private readonly ConcurrentDictionary<string, RecordSchema> _schemaCache = new();

        private readonly ISchemaRegistryClient _schemaRegistryClient;

        private AvroSerializer<GenericRecord> _avroSerializer;

        public ProducerService(ProducerConfig config, SchemaRegistryService schemaRegistryService)
        {
            _producer = new ProducerBuilder<byte[], byte[]>(config).Build();
            //_stringStringProducer = new ProducerBuilder<string, string>(config).Build();
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

            byte[] keyBytes;
            if (request.Key == null)
            {
                keyBytes = null;
            }
            else if (request.KeyType == KeyValueType.String)
            {
                keyBytes = Encoding.UTF8.GetBytes(request.Key);
            }
            else if (request.KeyType == KeyValueType.Bytes)
            {
                keyBytes = Convert.FromBase64String(request.Key);
            }
            else if (request.KeyType == KeyValueType.AvroAsXml)
            {
                var genericRecord =
                    ProducerGenericMapper.GetGenericRecord(request.Key, request.KeySchema, _schemaCache);
                keyBytes = await GetAvroSerializer().SerializeAsync(genericRecord,
                    new SerializationContext(MessageComponentType.Key, topic, headers));
            }
            else
            {
                throw new BadRequestException("Unexpected key serialization type: " + request.KeyType);
            }

            byte[] valueBytes;
            if (request.Value == null)
            {
                valueBytes = null;
            }
            else if (request.ValueType == KeyValueType.String)
            {
                valueBytes = Encoding.UTF8.GetBytes(request.Value);
            }
            else if (request.ValueType == KeyValueType.Bytes)
            {
                valueBytes = Convert.FromBase64String(request.Value);
            }
            else if (request.ValueType == KeyValueType.AvroAsXml)
            {
                var genericRecord =
                    ProducerGenericMapper.GetGenericRecord(request.Value, request.ValueSchema, _schemaCache);
                valueBytes = await GetAvroSerializer().SerializeAsync(genericRecord,
                    new SerializationContext(MessageComponentType.Value, topic, headers)).ConfigureAwait(false);
            }
            else
            {
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
                    : await _producer.ProduceAsync(topic, message);
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
                _producerSemaphore.Wait(TimeSpan.FromSeconds(10));
                try
                {
                    if (_schemaRegistryClient == null)
                        throw new BadRequestException("SchemaRegistry Client not initialized.");

                    _avroSerializer = new AvroSerializer<GenericRecord>(_schemaRegistryClient);
                }
                finally
                {
                    _producerSemaphore.Release();
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