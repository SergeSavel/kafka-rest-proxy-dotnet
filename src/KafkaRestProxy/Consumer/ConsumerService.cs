using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using pro.savel.KafkaRestProxy.Consumer.Contract;

namespace pro.savel.KafkaRestProxy.Consumer
{
    public class ConsumerService
    {
        private readonly ConsumerConfig _consumerConfig;

        public ConsumerService(ConsumerConfig consumerConfig)
        {
            _consumerConfig = consumerConfig;
        }

        public ConsumerProvider<string, string> ConsumerProvider { get; } = new(Deserializers.Utf8, Deserializers.Utf8);

        public ICollection<Contract.Consumer> ListConsumers()
        {
            return ConsumerProvider.ListConsumers()
                .Select(ConsumerMapper.Map)
                .ToList();
        }

        public Contract.Consumer CreateConsumer(string topic, int partition, long? offset, int expirationTimeout,
            string groupId)
        {
            var consumerConfig = _consumerConfig;
            if (groupId != null)
                consumerConfig = new ConsumerConfig(consumerConfig)
                {
                    GroupId = groupId
                };

            var consumerWrapper =
                ConsumerProvider.CreateConsumer(consumerConfig, TimeSpan.FromMilliseconds(expirationTimeout));

            if (consumerWrapper == null) return null;

            consumerWrapper.Assign(topic, partition);
            if (offset.HasValue) consumerWrapper.Seek(offset.Value);

            return ConsumerMapper.Map(consumerWrapper);
        }

        public Contract.Consumer GetConsumer(Guid consumerId)
        {
            var consumerWrapper = ConsumerProvider.GetConsumer(consumerId);
            if (consumerWrapper == null) return null;
            return ConsumerMapper.Map(consumerWrapper);
        }

        public bool RemoveConsumer(Guid consumerId)
        {
            return ConsumerProvider.RemoveConsumer(consumerId);
        }

        public ConsumerMessage Consume(Guid consumerId, int timeout)
        {
            var consumerWrapper = ConsumerProvider.GetConsumer(consumerId);

            if (consumerWrapper == null) return null;

            var consumeResult = consumerWrapper.Consume(timeout);

            return ConsumerMapper.Map(consumeResult);
        }
    }
}