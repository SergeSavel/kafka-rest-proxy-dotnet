using System;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using SergeSavel.KafkaRestProxy.Consumer.Contract;
using TopicPartition = SergeSavel.KafkaRestProxy.Consumer.Contract.TopicPartition;

namespace SergeSavel.KafkaRestProxy.Consumer
{
    public static class ConsumerMapper
    {
        public static Contract.Consumer Map(ConsumerWrapper source)
        {
            return new()
            {
                Id = source.Id,
                ExpiresAt = source.ExpiresAt,
                Creator = source.Creator
            };
        }

        public static ConsumerMessage Map(ConsumeResult<string, string> source)
        {
            if (source == null) return null;

            return new ConsumerMessage
            {
                Timestamp = source.Message.Timestamp.UnixTimestampMs,
                Topic = source.Topic,
                Partition = source.Partition,
                Offset = source.Offset,
                Key = source.Message?.Key,
                Headers = source.Message?.Headers
                    .ToDictionary(h => h.Key, h => MapHeaderBytes(h.GetValueBytes())),
                Value = source.Message?.Value,
                IsPartitionEOF = source.IsPartitionEOF
            };
        }

        private static string MapHeaderBytes(byte[] bytes)
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