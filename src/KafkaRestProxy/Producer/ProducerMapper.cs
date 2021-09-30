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
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using SergeSavel.KafkaRestProxy.Common.Exceptions;
using SergeSavel.KafkaRestProxy.Producer.Contract;
using SergeSavel.KafkaRestProxy.Producer.Requests;

namespace SergeSavel.KafkaRestProxy.Producer
{
    public static class ProducerMapper
    {
        [Obsolete]
        public static Message<string, string> MapMessageStringString(PostMessageRequest source)
        {
            var result = new Message<string, string>
            {
                Key = source.Key,
                Value = source.Value
            };

            if (source.Headers != null) result.Headers = MapHeaders(source.Headers);

            return result;
        }

        [Obsolete]
        public static Message<string, GenericRecord> MapMessageStringGeneric(PostMessageRequest source,
            ConcurrentDictionary<string, RecordSchema> schemaCache)
        {
            GenericRecord valueRecord;

            try
            {
                valueRecord = ProducerGenericMapper.GetGenericRecord(source.Value, source.ValueSchema, schemaCache);
            }
            catch (InvalidOperationException e)
            {
                throw new BadRequestException(e.Message);
            }

            var result = new Message<string, GenericRecord>
            {
                Key = source.Key,
                Value = valueRecord
            };

            if (source.Headers != null) result.Headers = MapHeaders(source.Headers);

            return result;
        }

        [Obsolete]
        public static Message<GenericRecord, GenericRecord> MapMessageGenericGeneric(PostMessageRequest source,
            ConcurrentDictionary<string, RecordSchema> schemaCache)
        {
            var keyRecord = ProducerGenericMapper.GetGenericRecord(source.Key, source.KeySchema, schemaCache);
            var valueRecord = ProducerGenericMapper.GetGenericRecord(source.Value, source.ValueSchema, schemaCache);

            var result = new Message<GenericRecord, GenericRecord>
            {
                Key = keyRecord,
                Value = valueRecord
            };

            if (source.Headers != null) result.Headers = MapHeaders(source.Headers);

            return result;
        }

        [Obsolete]
        public static Message<GenericRecord, string> MapMessageGenericString(PostMessageRequest source,
            ConcurrentDictionary<string, RecordSchema> schemaCache)
        {
            var keyRecord = ProducerGenericMapper.GetGenericRecord(source.Key, source.KeySchema, schemaCache);

            var result = new Message<GenericRecord, string>
            {
                Key = keyRecord,
                Value = source.Value
            };

            if (source.Headers != null) result.Headers = MapHeaders(source.Headers);

            return result;
        }

        public static Headers MapHeaders(IEnumerable<KeyValuePair<string, string>> source)
        {
            var result = new Headers();

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

        public static DeliveryResult Map<TKey, TValue>(DeliveryResult<TKey, TValue> source)
        {
            return new()
            {
                Status = Enum.GetName(source.Status),
                Topic = source.Topic,
                Partition = source.Partition.Value,
                Offset = source.Offset,
                Timestamp = source.Timestamp.UnixTimestampMs
            };
        }
    }
}