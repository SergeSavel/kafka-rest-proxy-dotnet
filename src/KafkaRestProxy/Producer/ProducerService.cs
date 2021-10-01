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

        [Obsolete] private readonly IProducer<string, string> _stringStringProducer;

        private AvroSerializer<GenericRecord> _avroSerializer;
        [Obsolete] private IProducer<GenericRecord, GenericRecord> _dependentGenericGenericProducer;
        [Obsolete] private IProducer<GenericRecord, string> _dependentGenericStringProducer;
        [Obsolete] private IProducer<string, GenericRecord> _dependentStringGenericProducer;

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

        [Obsolete]
        private async Task<DeliveryResult> PostStringString(string topic, int? partition, PostMessageRequest request)
        {
            var producerMessage = ProducerMapper.MapMessageStringString(request);

            var producer = _stringStringProducer;

            DeliveryResult<string, string> producerDeliveryResult;
            try
            {
                producerDeliveryResult = partition.HasValue
                    ? await producer.ProduceAsync(new TopicPartition(topic, partition.Value), producerMessage)
                    : await producer.ProduceAsync(topic, producerMessage);
            }
            catch (KafkaException e)
            {
                throw new ProduceException(e);
            }

            var deliveryResult = ProducerMapper.Map(producerDeliveryResult);

            return deliveryResult;
        }

        [Obsolete]
        private async Task<DeliveryResult> PostStringAvro(string topic, int? partition, PostMessageRequest request)
        {
            var producerMessage = ProducerMapper.MapMessageStringGeneric(request, _schemaCache);

            var producer = GetStringAvroProducer();

            DeliveryResult<string, GenericRecord> producerDeliveryResult;
            try
            {
                producerDeliveryResult = partition.HasValue
                    ? await producer.ProduceAsync(new TopicPartition(topic, partition.Value), producerMessage)
                    : await producer.ProduceAsync(topic, producerMessage);
            }
            catch (KafkaException e)
            {
                throw new ProduceException(e);
            }

            var deliveryResult = ProducerMapper.Map(producerDeliveryResult);

            return deliveryResult;
        }

        [Obsolete]
        private async Task<DeliveryResult> PostAvroAvro(string topic, int? partition, PostMessageRequest request)
        {
            var producerMessage = ProducerMapper.MapMessageGenericGeneric(request, _schemaCache);

            var producer = GetAvroAvroProducer();

            DeliveryResult<GenericRecord, GenericRecord> producerDeliveryResult;
            try
            {
                producerDeliveryResult = partition.HasValue
                    ? await producer.ProduceAsync(new TopicPartition(topic, partition.Value), producerMessage)
                    : await producer.ProduceAsync(topic, producerMessage);
            }
            catch (KafkaException e)
            {
                throw new ProduceException(e);
            }

            var deliveryResult = ProducerMapper.Map(producerDeliveryResult);

            return deliveryResult;
        }

        [Obsolete]
        private async Task<DeliveryResult> PostAvroString(string topic, int? partition, PostMessageRequest request)
        {
            var producerMessage = ProducerMapper.MapMessageGenericString(request, _schemaCache);

            var producer = GetAvroStringProducer();

            DeliveryResult<GenericRecord, string> producerDeliveryResult;
            try
            {
                producerDeliveryResult = partition.HasValue
                    ? await producer.ProduceAsync(new TopicPartition(topic, partition.Value), producerMessage)
                    : await producer.ProduceAsync(topic, producerMessage);
            }
            catch (KafkaException e)
            {
                throw new ProduceException(e);
            }

            var deliveryResult = ProducerMapper.Map(producerDeliveryResult);

            return deliveryResult;
        }

        [Obsolete]
        private IProducer<string, GenericRecord> GetStringAvroProducer()
        {
            if (_dependentStringGenericProducer == null)
            {
                _producerSemaphore.Wait(TimeSpan.FromSeconds(10));

                try
                {
                    if (_dependentStringGenericProducer == null)
                    {
                        if (_schemaRegistryClient == null)
                            throw new InvalidOperationException("SchemaRegistry Client not initialized.");

                        _dependentStringGenericProducer = new DependentProducerBuilder<string, GenericRecord>(Handle)
                            .SetValueSerializer(new AvroSerializer<GenericRecord>(_schemaRegistryClient))
                            .Build();
                    }
                }
                finally
                {
                    _producerSemaphore.Release();
                }
            }

            return _dependentStringGenericProducer;
        }

        [Obsolete]
        private IProducer<GenericRecord, GenericRecord> GetAvroAvroProducer()
        {
            if (_dependentGenericGenericProducer == null)
            {
                _producerSemaphore.Wait(TimeSpan.FromSeconds(10));

                try
                {
                    if (_dependentGenericGenericProducer == null)
                    {
                        if (_schemaRegistryClient == null)
                            throw new InvalidOperationException("SchemaRegistry Client not initialized.");

                        _dependentGenericGenericProducer =
                            new DependentProducerBuilder<GenericRecord, GenericRecord>(Handle)
                                .SetKeySerializer(new AvroSerializer<GenericRecord>(_schemaRegistryClient))
                                .SetValueSerializer(new AvroSerializer<GenericRecord>(_schemaRegistryClient))
                                .Build();
                    }
                }
                finally
                {
                    _producerSemaphore.Release();
                }
            }

            return _dependentGenericGenericProducer;
        }

        [Obsolete]
        private IProducer<GenericRecord, string> GetAvroStringProducer()
        {
            if (_dependentGenericStringProducer == null)
            {
                _producerSemaphore.Wait(TimeSpan.FromSeconds(10));

                try
                {
                    if (_dependentGenericStringProducer == null)
                    {
                        if (_schemaRegistryClient == null)
                            throw new InvalidOperationException("SchemaRegistry Client not initialized.");

                        _dependentGenericStringProducer = new DependentProducerBuilder<GenericRecord, string>(Handle)
                            .SetKeySerializer(new AvroSerializer<GenericRecord>(_schemaRegistryClient))
                            .Build();
                    }
                }
                finally
                {
                    _producerSemaphore.Release();
                }
            }

            return _dependentGenericStringProducer;
        }

        [Obsolete]
        internal IAsyncSerializer<GenericRecord> GetGenericSerializer(KeyValueType serializationType)
        {
            if (serializationType == KeyValueType.AvroAsXml)
                return GetAvroSerializer();


            throw new BadRequestException("Unexpected serialization type:" + serializationType);
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
                if (_dependentStringGenericProducer != null)
                {
                    _dependentStringGenericProducer.Flush();
                    _dependentStringGenericProducer.Dispose();
                }

                if (_dependentGenericGenericProducer != null)
                {
                    _dependentGenericGenericProducer.Flush();
                    _dependentGenericGenericProducer.Dispose();
                }

                if (_dependentGenericStringProducer != null)
                {
                    _dependentGenericStringProducer.Flush();
                    _dependentGenericStringProducer.Dispose();
                }

                _stringStringProducer.Flush();
                _stringStringProducer.Dispose();

                _producer.Flush();
                _producer.Dispose();
            }
        }
    }
}