using System;
using Confluent.Kafka;

namespace pro.savel.KafkaRestProxy.Consumer
{
    public class ConsumerWrapper<TKey, TValue> : IDisposable
    {
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly TimeSpan _expirationTimeout;

        public ConsumerWrapper(ConsumerConfig consumerConfig, TimeSpan expirationTimeout,
            IDeserializer<TKey> keyDeserializer = null, IDeserializer<TValue> valueDeserializer = null)
        {
            _expirationTimeout = expirationTimeout;
            UpdateExpiration();

            _consumer = new ConsumerBuilder<TKey, TValue>(consumerConfig)
                .SetKeyDeserializer(keyDeserializer)
                .SetValueDeserializer(valueDeserializer)
                .Build();
        }

        public TopicPartition TopicPartition { get; private set; }

        public long Position => _consumer.Position(TopicPartition);

        public WatermarkOffsets WatermarkOffsets => _consumer.GetWatermarkOffsets(TopicPartition);

        public Guid Id { get; } = Guid.NewGuid();

        public DateTime ExpiresAt { get; private set; }

        public TimeSpan ExpiresAfter => ExpiresAt - DateTime.Now;

        public void Dispose()
        {
            _consumer.Dispose();
        }

        public void Assign(string topic, int partition)
        {
            UpdateExpiration();

            TopicPartition = new TopicPartition(topic, partition);
            _consumer.Assign(TopicPartition);
        }

        public void Seek(long offset)
        {
            UpdateExpiration();

            if (TopicPartition == null)
                throw new InvalidOperationException("The consumer is not assigned to topic/partition.");
            _consumer.Seek(new TopicPartitionOffset(TopicPartition, offset));
        }

        public ConsumeResult<TKey, TValue> Consume(int millisecondsTimeout)
        {
            UpdateExpiration();

            return _consumer.Consume(millisecondsTimeout);
        }

        public ConsumeResult<TKey, TValue> Consume(TimeSpan timeout)
        {
            return Consume(timeout.Milliseconds);
        }

        public void UpdateExpiration()
        {
            ExpiresAt = DateTime.Now + _expirationTimeout;
        }
    }
}