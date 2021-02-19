using System;
using System.Text;
using Confluent.Kafka;
using pro.savel.KafkaRestProxy.Producer.Contract;

namespace pro.savel.KafkaRestProxy.Producer
{
    public static class ProducerMapper
    {
        public static Message<string, string> MapMessage(Message source)
        {
            var result = new Message<string, string>
            {
                Key = source.Key,
                Value = source.Value
            };

            if (source.Headers != null)
                foreach (var (key, stringValue) in source.Headers)
                    if (key != null)
                    {
                        byte[] value = null;
                        if (stringValue != null)
                            value = Encoding.UTF8.GetBytes(stringValue);
                        result.Headers.Add(key, value);
                    }

            return result;
        }

        public static DeliveryResult MapDeliveryResult<TKey, TValue>(DeliveryResult<TKey, TValue> source)
        {
            var status =
                source.Status switch
                {
                    PersistenceStatus.NotPersisted => DeliveryResult.PersistenceStatus.NotPersisted,
                    PersistenceStatus.Persisted => DeliveryResult.PersistenceStatus.Persisted,
                    PersistenceStatus.PossiblyPersisted => DeliveryResult.PersistenceStatus.PossiblyPersisted,
                    _ => throw new ArgumentException(nameof(source.Status))
                };

            return new DeliveryResult
            {
                Status = status,
                PartitionId = source.Partition.Value,
                Offset = source.Offset,
                Timestamp = source.Timestamp.UnixTimestampMs
            };
        }
    }
}