﻿using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using pro.savel.KafkaRestProxy.Consumer.Contract;
using pro.savel.KafkaRestProxy.Consumer.Exceptions;
using pro.savel.KafkaRestProxy.Consumer.Requests;
using TopicPartition = pro.savel.KafkaRestProxy.Consumer.Contract.TopicPartition;
using WatermarkOffsets = pro.savel.KafkaRestProxy.Consumer.Contract.WatermarkOffsets;

namespace pro.savel.KafkaRestProxy.Consumer
{
    public class ConsumerService
    {
        private readonly ConsumerConfig _consumerConfig;

        public ConsumerService(ConsumerConfig consumerConfig)
        {
            _consumerConfig = consumerConfig;
        }

        public ConsumerProvider ConsumerProvider { get; } = new();

        public List<Contract.Consumer> ListConsumers()
        {
            return ConsumerProvider.ListConsumers()
                .Select(ConsumerMapper.Map)
                .ToList();
        }

        public Contract.Consumer CreateConsumer(CreateConsumerRequest request)
        {
            var consumerConfig = _consumerConfig;
            if (request.GroupId != null)
                consumerConfig = new ConsumerConfig(consumerConfig)
                {
                    GroupId = request.GroupId
                };

            var wrapper =
                ConsumerProvider.CreateConsumer(
                    consumerConfig.ToDictionary(kv => kv.Key, kv => kv.Value),
                    TimeSpan.FromMilliseconds(request.ExpirationTimeoutMs));

            //consumerWrapper.Assign(request.Topic, request.Partitions.Select(ConsumerMapper.Map).ToList());

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

        public IEnumerable<ConsumerMessage> Consume(Guid consumerId, int? timeout)
        {
            var wrapper = ConsumerProvider.GetConsumer(consumerId);
            if (wrapper == null) throw new ConsumerNotFoundException(consumerId);

            wrapper.UpdateExpiration();

            var consumeResult = timeout.HasValue
                ? wrapper.Consumer.Consume(timeout.Value)
                : wrapper.Consumer.Consume();

            if (consumeResult == null)
                return Array.Empty<ConsumerMessage>();

            return new[] {ConsumerMapper.Map(consumeResult)};
        }

        public List<WatermarkOffsets> QueryWatermarkOffsets(Guid consumerId, int timeout)
        {
            var wrapper = ConsumerProvider.GetConsumer(consumerId);
            if (wrapper == null) throw new ConsumerNotFoundException(consumerId);

            var result = wrapper.Consumer.Assignment
                .Select(tp => new
                {
                    TopicPartition = tp,
                    WatermarkOffsets = wrapper.Consumer.QueryWatermarkOffsets(tp, TimeSpan.FromMilliseconds(timeout))
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