// Copyright 2021 Sergey Savelev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using SergeSavel.KafkaRestProxy.Consumer.Requests;
using SergeSavel.KafkaRestProxy.Consumer.Responses;
using SergeSavel.KafkaRestProxy.SchemaRegistry;
using ConsumeException = SergeSavel.KafkaRestProxy.Consumer.Exceptions.ConsumeException;
using TopicPartition = SergeSavel.KafkaRestProxy.Consumer.Responses.TopicPartition;

namespace SergeSavel.KafkaRestProxy.Consumer
{
    public class ConsumerService : IDisposable
    {
        private readonly ConsumerConfig _consumerConfig;
        private readonly ILogger<ConsumerService> _logger;
        private readonly IConsumer<string, string> _staticConsumer;

        public ConsumerService(ConsumerConfig consumerConfig, ILogger<ConsumerService> logger,
            SchemaRegistryService schemaRegistryService)
        {
            _consumerConfig = consumerConfig;
            _logger = logger;
            _staticConsumer = new ConsumerBuilder<string, string>(consumerConfig)
                .Build();
            ConsumerProvider = new ConsumerProvider(schemaRegistryService.Client);
        }

        public ConsumerProvider ConsumerProvider { get; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                ConsumerProvider.Dispose();

                _staticConsumer.Close();
                _staticConsumer.Dispose();
            }
        }

        public ICollection<Responses.Consumer> ListConsumers()
        {
            return ConsumerProvider.ListConsumers()
                .Select(ConsumerMapper.Map)
                .ToList();
        }

        public Responses.Consumer CreateConsumer(CreateConsumerRequest request, string creator)
        {
            var config = _consumerConfig.ToDictionary(kv => kv.Key, kv => kv.Value);

            if (request.Config != null)
                foreach (var (key, value) in request.Config)
                    config[key] = value;

            var wrapper =
                ConsumerProvider.CreateConsumer(config, TimeSpan.FromMilliseconds(request.ExpirationTimeoutMs),
                    request.KeyType, request.ValueType, creator);

            return ConsumerMapper.Map(wrapper);
        }

        public Responses.Consumer GetConsumer(Guid consumerId)
        {
            var wrapper = ConsumerProvider.GetConsumer(consumerId);

            wrapper.UpdateExpiration();

            return ConsumerMapper.Map(wrapper);
        }

        public bool RemoveConsumer(Guid consumerId)
        {
            return ConsumerProvider.RemoveConsumer(consumerId);
        }

        public ICollection<TopicPartition> AssignConsumer(AssignConsumerRequest request)
        {
            var wrapper = ConsumerProvider.GetConsumer(request.ConsumerId);

            wrapper.UpdateExpiration();

            wrapper.Consumer
                .Assign(request.Partitions.Select(p => new TopicPartitionOffset(p.Topic, p.Partition, p.Offset)));

            return GetConsumerAssignment(request.ConsumerId);
        }

        public ICollection<TopicPartition> GetConsumerAssignment(Guid consumerId)
        {
            var wrapper = ConsumerProvider.GetConsumer(consumerId);

            wrapper.UpdateExpiration();

            var result = wrapper.Consumer.Assignment
                .Select(ConsumerMapper.Map)
                .ToList();

            return result;
        }

        public ConsumerMessage Consume(Guid consumerId, int? timeout)
        {
            var wrapper = ConsumerProvider.GetConsumer(consumerId);

            wrapper.UpdateExpiration();

            ConsumeResult<string, string> consumeResult;
            try
            {
                consumeResult = timeout.HasValue
                    ? wrapper.Consumer.Consume(timeout.Value)
                    : wrapper.Consumer.Consume();
            }
            catch (KafkaException e)
            {
                throw new ConsumeException("Unable to receive message.", e);
            }

            return ConsumerMapper.Map(consumeResult);
        }

        [Obsolete]
        public IEnumerable<ConsumerMessage> ConsumeMultiple(Guid consumerId, int? timeout, int? limit)
        {
            var wrapper = ConsumerProvider.GetConsumer(consumerId);

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
                catch (KafkaException e)
                {
                    throw new ConsumeException("Unable to receive message.", e);
                }
            } while (--limit > 0);

            return result;
        }

        public PartitionOffsets GetPartitionOffsets(Guid consumerId, string topic, int partition, int? timeout)
        {
            var eventId = new EventId(Thread.CurrentThread.ManagedThreadId, nameof(GetPartitionOffsets));

            var wrapper = ConsumerProvider.GetConsumer(consumerId);

            wrapper.UpdateExpiration();

            Confluent.Kafka.TopicPartition topicPartition;
            WatermarkOffsets watermarkOffsets;
            Offset position;

            try
            {
                topicPartition = new Confluent.Kafka.TopicPartition(topic, partition);

                if (timeout.HasValue)
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug(eventId,
                            "Start calling 'QueryWatermarkOffsets()'. Topic='{Topic}', Partition='{Partition}', Timeout='{Timeout}'",
                            topic, partition, timeout);

                    var stopwatch = Stopwatch.StartNew();

                    watermarkOffsets =
                        _staticConsumer.QueryWatermarkOffsets(topicPartition,
                            TimeSpan.FromMilliseconds(timeout.Value));

                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug(eventId, "End calling 'QueryWatermarkOffsets()'. Took {Ms} ms",
                            stopwatch.ElapsedMilliseconds);
                }
                else
                {
                    watermarkOffsets = _staticConsumer.GetWatermarkOffsets(topicPartition);
                }
            }
            catch (KafkaException e)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug("Error: {Reason}", e.Error.Reason);

                throw new ConsumeException("Unable to get partition offsets.", e);
            }

            position = wrapper.Consumer.Position(topicPartition);

            return new PartitionOffsets
            {
                Topic = topicPartition.Topic,
                Partition = topicPartition.Partition,
                High = watermarkOffsets.High,
                Low = watermarkOffsets.Low,
                Current = ConsumerMapper.Map(position)
            };
        }
    }
}