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
using System.Linq;
using System.Text;
using Confluent.Kafka;
using SergeSavel.KafkaRestProxy.Consumer.Responses;
using TopicPartition = SergeSavel.KafkaRestProxy.Consumer.Responses.TopicPartition;

namespace SergeSavel.KafkaRestProxy.Consumer
{
    public static class ConsumerMapper
    {
        public static Responses.Consumer Map(ConsumerWrapper source)
        {
            return new()
            {
                Id = source.Id,
                ExpiresAt = source.ExpiresAt,
                Creator = source.Creator,
                KeyType = Enum.GetName(source.KeyType),
                ValueType = Enum.GetName(source.ValueType)
            };
        }

        public static ConsumerMessage Map(ConsumeResult<string, string> source)
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
                    .ToDictionary(h => h.Key, h => MapHeaderBytes(h.GetValueBytes())),
                Value = source.Message?.Value,
                IsPartitionEOF = source.IsPartitionEOF
            };
        }

        public static string MapHeaderBytes(byte[] bytes)
        {
            if (bytes == null) return null;
            return Encoding.UTF8.GetString(bytes);
        }

        public static TopicPartition Map(Confluent.Kafka.TopicPartition source)
        {
            return new()
            {
                Topic = source.Topic,
                Partition = source.Partition
            };
        }

        public static PartitionOffsets.Offset Map(Offset offset)
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
    }
}