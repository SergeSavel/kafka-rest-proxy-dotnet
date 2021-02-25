using System.Linq;
using System.Text;
using Confluent.Kafka;
using pro.savel.KafkaRestProxy.Consumer.Contract;

namespace pro.savel.KafkaRestProxy.Consumer
{
    public static class ConsumerMapper
    {
        public static Contract.Consumer Map(ConsumerWrapper<string, string> source)
        {
            return new()
            {
                Id = source.Id,
                Topic = source.TopicPartition.Topic,
                Partition = source.TopicPartition.Partition,
                Position = source.Position,
                ExpiresAt = source.ExpiresAt
            };
        }

        public static ConsumerMessage Map(ConsumeResult<string, string> source)
        {
            if (source == null) return new ConsumerMessage();

            return new ConsumerMessage()
            {
                Timestamp = source.Message.Timestamp.UnixTimestampMs,
                Offset = source.Offset,
                Key = source.Message.Key,
                Headers = source.Message.Headers
                    .ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes())),
                Value = source.Message.Value
            };
        }
    }
}