using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Confluent.Kafka;

namespace pro.savel.KafkaRestProxy.Consumer
{
    public class ConsumerProvider<TKey, TValue>
    {
        private readonly ConcurrentDictionary<Guid, ConsumerWrapper<TKey, TValue>> _consumers = new();
        private readonly IDeserializer<TKey> _keyDeserializer;
        private readonly IDeserializer<TValue> _valueDeserializer;

        public ConsumerProvider(IDeserializer<TKey> keyDeserializer, IDeserializer<TValue> valueDeserializer)
        {
            _keyDeserializer = keyDeserializer;
            _valueDeserializer = valueDeserializer;
        }

        public ConsumerWrapper<TKey, TValue> CreateConsumer(ConsumerConfig consumerConfig, TimeSpan expirationTimeout)
        {
            var consumerWrapper = new ConsumerWrapper<TKey, TValue>(consumerConfig, expirationTimeout,
                _keyDeserializer, _valueDeserializer);

            if (!_consumers.TryAdd(consumerWrapper.Id, consumerWrapper))
            {
                consumerWrapper.Dispose();
                return null;
            }

            return consumerWrapper;
        }

        public ConsumerWrapper<TKey, TValue> GetConsumer(Guid id)
        {
            return _consumers.TryGetValue(id, out var consumerWrapper) ? consumerWrapper : null;
        }

        public ICollection<ConsumerWrapper<TKey, TValue>> ListConsumers()
        {
            return _consumers.Values;
        }

        public bool RemoveConsumer(Guid id)
        {
            if (!_consumers.TryRemove(id, out var consumerWrapper)) return false;
            consumerWrapper.Dispose();
            return true;
        }

        public void RemoveExpiredConsumers()
        {
            foreach (var consumerWrapper in _consumers.Values)
                if (consumerWrapper.ExpiresAfter <= TimeSpan.Zero)
                    RemoveConsumer(consumerWrapper.Id);
        }
    }
}