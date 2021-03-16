using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using pro.savel.KafkaRestProxy.Consumer.Contract;
using pro.savel.KafkaRestProxy.Consumer.Exceptions;
using pro.savel.KafkaRestProxy.Consumer.Requests;
using ConsumeException = Confluent.Kafka.ConsumeException;
using TopicPartition = pro.savel.KafkaRestProxy.Consumer.Contract.TopicPartition;
using WatermarkOffsets = pro.savel.KafkaRestProxy.Consumer.Contract.WatermarkOffsets;

namespace pro.savel.KafkaRestProxy.Consumer
{
    public class ConsumerService : IDisposable
    {
        private readonly ConsumerConfig _consumerConfig;

        public ConsumerService(ConsumerConfig consumerConfig)
        {
            _consumerConfig = consumerConfig;
        }

        public ConsumerProvider ConsumerProvider { get; } = new();

        public void Dispose()
        {
            ConsumerProvider.Dispose();
        }

        public List<Contract.Consumer> ListConsumers()
        {
            return ConsumerProvider.ListConsumers()
                .Select(ConsumerMapper.Map)
                .ToList();
        }

        public Contract.Consumer CreateConsumer(CreateConsumerRequest request)
        {
            var config = _consumerConfig.ToDictionary(kv => kv.Key, kv => kv.Value);

            if (request.Config != null)
                foreach (var (key, value) in request.Config)
                    config[key] = value;

            var wrapper =
                ConsumerProvider.CreateConsumer(config, TimeSpan.FromMilliseconds(request.ExpirationTimeoutMs));

            return ConsumerMapper.Map(wrapper);
        }

        public Contract.Consumer GetConsumer(Guid consumerId)
        {
            var wrapper = ConsumerProvider.GetConsumer(consumerId);
            if (wrapper == null) throw new ConsumerNotFoundException(consumerId);
            return ConsumerMapper.Map(wrapper);
        }

        public bool RemoveConsumer(Guid consumerId)
        {
            return ConsumerProvider.RemoveConsumer(consumerId);
        }

        public List<TopicPartition> AssignConsumer(AssignConsumerRequest request)
        {
            var wrapper = ConsumerProvider.GetConsumer(request.ConsumerId);
            if (wrapper == null) throw new ConsumerNotFoundException(request.ConsumerId);

            wrapper.UpdateExpiration();

            wrapper.Consumer
                .Assign(request.Partitions.Select(p => new TopicPartitionOffset(p.Topic, p.Partition, p.Offset)));

            return GetConsumerAssignment(request.ConsumerId);
        }

        public List<TopicPartition> GetConsumerAssignment(Guid consumerId)
        {
            var wrapper = ConsumerProvider.GetConsumer(consumerId);
            if (wrapper == null) throw new ConsumerNotFoundException(consumerId);

            var result = wrapper.Consumer.Assignment
                .Select(ConsumerMapper.Map)
                .ToList();

            return result;
        }

        public IEnumerable<ConsumerMessage> Consume(Guid consumerId, int? timeout, int? limit)
        {
            var wrapper = ConsumerProvider.GetConsumer(consumerId);
            if (wrapper == null) throw new ConsumerNotFoundException(consumerId);

            wrapper.UpdateExpiration();

            var result = new List<ConsumerMessage>(limit ?? 1);

            do
            {
                try
                {
                    var consumeResult = timeout.HasValue
                        ? wrapper.Consumer.Consume(timeout.Value)
                        : wrapper.Consumer.Consume();
                    if (consumeResult == null) break;
                    result.Add(ConsumerMapper.Map(consumeResult));
                }
                catch (ConsumeException e)
                {
                    throw new Exceptions.ConsumeException(e)
                    {
                        Value = e.Error
                    };
                }
            } while (--limit > 0);

            return result;
        }

        public ICollection<WatermarkOffsets> GetWatermarkOffsets(Guid consumerId, int? timeout)
        {
            var wrapper = ConsumerProvider.GetConsumer(consumerId);
            if (wrapper == null) throw new ConsumerNotFoundException(consumerId);

            var result = wrapper.Consumer.Assignment
                .Select(tp => new
                {
                    TopicPartition = tp,
                    WatermarkOffsets = timeout.HasValue
                        ? wrapper.Consumer.QueryWatermarkOffsets(tp, TimeSpan.FromMilliseconds(timeout.Value))
                        : wrapper.Consumer.GetWatermarkOffsets(tp)
                })
                .Select(o => new WatermarkOffsets
                {
                    Topic = o.TopicPartition.Topic,
                    Partition = o.TopicPartition.Partition,
                    Low = o.WatermarkOffsets.Low,
                    High = o.WatermarkOffsets.High
                })
                .ToList();

            return result;
        }
    }
}