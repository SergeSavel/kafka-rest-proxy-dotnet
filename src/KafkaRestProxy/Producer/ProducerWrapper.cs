// Copyright 2023 Sergey Savelev
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Concurrent;
using System.Text;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.Common.Contract;
using SergeSavel.KafkaRestProxy.Common.Exceptions;
using SergeSavel.KafkaRestProxy.Common.Extensions;
using SergeSavel.KafkaRestProxy.Producer.Contract;
using SergeSavel.KafkaRestProxy.Producer.Exceptions;
using SergeSavel.KafkaRestProxy.Producer.Responses;
using KafkaException = Confluent.Kafka.KafkaException;
using Schema = Avro.Schema;

namespace SergeSavel.KafkaRestProxy.Producer;

public class ProducerWrapper : ClientWrapper
{
    private static readonly ISerializer<byte[]> XSerializer = new RawSerializer();
    private readonly ConcurrentDictionary<string, RecordSchema> _avroSchemaCache = new();
    private readonly IProducer<byte[], byte[]> _producer;

    private readonly ISchemaRegistryClient _schemaRegistryClient;

    private readonly SemaphoreSlim _semaphore = new(1);
    private AvroSerializer<GenericRecord> _avroSerializer;

    public ProducerWrapper(string name, IDictionary<string, string> config, KeyValueType keyType,
        KeyValueType valueType,
        ISchemaRegistryClient schemaRegistryClient, TimeSpan expirationTimeout) : base(name, config,
        expirationTimeout)
    {
        KeyType = keyType;
        ValueType = valueType;
        _schemaRegistryClient = schemaRegistryClient;
        try
        {
            _producer = new ProducerBuilder<byte[], byte[]>(config)
                .SetKeySerializer(XSerializer)
                .SetValueSerializer(XSerializer)
                .Build();
        }
        catch (Exception e)
        {
            throw new ClientConfigException(e);
        }
    }

    public KeyValueType KeyType { get; }
    public KeyValueType ValueType { get; }

    public Task SetSchemaAsync(string schemaString)
    {
        if (KeyType == KeyValueType.Avro || ValueType == KeyValueType.Avro)
        {
            RecordSchema schema;
            try
            {
                schema = (RecordSchema)Schema.Parse(schemaString);
            }
            catch (SchemaParseException e)
            {
                throw new SerializationException("An error occured while parsing schema", e);
            }

            var schemaFullName = schema.Fullname.ToUpperInvariant();
            _avroSchemaCache.AddOrUpdate(schemaFullName, _ => schema, (_, _) => schema);
        }
        else
        {
            throw new SerializationException("Schemas not supported for this producer.") { StatusCode = 400 };
        }

        return Task.CompletedTask;
    }

    public async Task<DeliveryResult> ProduceAsync(string topic, int? partition, IMessage message)
    {
        var kafkaHeaders = Map(message.Headers);

        var keyContext = new SerializationContext(MessageComponentType.Key, topic, kafkaHeaders);
        var keyBytes = await SerializeAsync(keyContext, message.Key, KeyType)
            .ConfigureAwait(false);

        var valueContext = new SerializationContext(MessageComponentType.Value, topic, kafkaHeaders);
        var valueBytes = await SerializeAsync(valueContext, message.Value, ValueType)
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
        KeyValueType dataType)
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
            case KeyValueType.Avro:
            {
                GenericRecord genericRecord;
                try
                {
                    genericRecord = stringData.AsAvroGenericRecord(_avroSchemaCache);
                }
                catch (Exception e)
                {
                    throw new SerializationException("An error occured while parsing Avro generic record.", e);
                }

                try
                {
                    result = await GetAvroSerializer().SerializeAsync(genericRecord, serializationContext)
                        .ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new SerializationException("An error occured while serializing Avro generic record.", e);
                }

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
            Partition = source.Partition,
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
        catch (Exception)
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