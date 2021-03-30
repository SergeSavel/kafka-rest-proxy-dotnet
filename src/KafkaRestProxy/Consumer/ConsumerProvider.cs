using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace SergeSavel.KafkaRestProxy.Consumer
{
    public class ConsumerProvider : IDisposable
    {
        private readonly ConcurrentDictionary<Guid, ConsumerWrapper> _consumers = new();

        public void Dispose()
        {
            var wrappers = ListConsumers();
            foreach (var wrapper in wrappers)
                try
                {
                    wrapper.Dispose();
                }
                catch (ObjectDisposedException)
                {
                }
        }

        public ConsumerWrapper CreateConsumer(IDictionary<string, string> consumerConfig, TimeSpan expirationTimeout,
            string creator = null)
        {
            var consumerWrapper = new ConsumerWrapper(consumerConfig, expirationTimeout)
            {
                Creator = creator
            };

            if (!_consumers.TryAdd(consumerWrapper.Id, consumerWrapper))
            {
                consumerWrapper.Dispose();
                throw new Exception("Unable to register consumer.");
            }

            return consumerWrapper;
        }

        public ConsumerWrapper GetConsumer(Guid id)
        {
            return _consumers.TryGetValue(id, out var consumerWrapper) ? consumerWrapper : null;
        }

        public ICollection<ConsumerWrapper> ListConsumers()
        {
            return _consumers.Values.ToList();
        }

        public bool RemoveConsumer(Guid id)
        {
            if (!_consumers.TryRemove(id, out var consumerWrapper)) return false;
            consumerWrapper.Consumer.Close();
            consumerWrapper.Dispose();
            return true;
        }

        public void RemoveExpiredConsumers()
        {
            foreach (var consumerWrapper in _consumers.Values)
                if (consumerWrapper.IsExpired)
                    RemoveConsumer(consumerWrapper.Id);
        }
    }
}