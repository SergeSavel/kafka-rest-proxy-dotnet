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
using System.Text;
using Confluent.Kafka;
using SergeSavel.KafkaRestProxy.Producer.Contract;

namespace SergeSavel.KafkaRestProxy.Producer
{
    public static class ProducerMapper
    {
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