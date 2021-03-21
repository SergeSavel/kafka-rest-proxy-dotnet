using System;
using System.Text;
using Confluent.Kafka;
using pro.savel.KafkaRestProxy.Producer.Contract;
using pro.savel.KafkaRestProxy.Producer.Requests;

namespace pro.savel.KafkaRestProxy.Producer
{
    public static class ProducerMapper
    {
        public static Message<string, string> Map(PostMessageRequest source)
        {
            var result = new Message<string, string>
            {
                Key = source.Key,
                Value = source.Value
            };

            if (source.Headers != null)
            {
                result.Headers = new Headers();
                foreach (var (key, stringValue) in source.Headers)
                    if (key != null)
                    {
                        byte[] value = null;
                        if (stringValue != null)
                            value = Encoding.UTF8.GetBytes(stringValue);
                        result.Headers.Add(key, value);
                    }
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