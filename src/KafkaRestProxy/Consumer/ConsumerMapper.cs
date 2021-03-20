using System.Linq;
using System.Text;
using Confluent.Kafka;
using pro.savel.KafkaRestProxy.Consumer.Contract;
using TopicPartition = pro.savel.KafkaRestProxy.Consumer.Contract.TopicPartition;

namespace pro.savel.KafkaRestProxy.Consumer
{
    public static class ConsumerMapper
    {
        public static Contract.Consumer Map(ConsumerWrapper source)
        {
            return new()
            {
                Id = source.Id,
                ExpiresAt = source.ExpiresAt
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
                Key = source.Message.Key,
                Headers = source.Message.Headers
                    .ToDictionary(h => h.Key, h => MapHeaderBytes(h.GetValueBytes())),
                Value = source.Message.Value
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
    }
}