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
using System.Linq;
using SergeSavel.KafkaRestProxy.Consumer.Requests;
using SergeSavel.KafkaRestProxy.Consumer.Responses;

namespace SergeSavel.KafkaRestProxy.Consumer
{
    public class ConsumerService
    {
        private readonly ConsumerProvider _provider;

        public ConsumerService(ConsumerProvider provider)
        {
            _provider = provider;
        }

        public ICollection<Responses.Consumer> ListConsumers()
        {
            var wrappers = _provider.ListItems();
            return wrappers
                .Select(MapConsumer)
                .ToList();
        }

        public Responses.Consumer GetConsumer(Guid consumerId)
        {
            var wrapper = _provider.GetItem(consumerId);
            return MapConsumer(wrapper);
        }

        public ConsumerWithToken CreateConsumer(CreateConsumerRequest request, string owner)
        {
            var wrapper = _provider.CreateConsumer(request.Name, request.Config, request.KeyType, request.ValueType,
                TimeSpan.FromMilliseconds(request.ExpirationTimeoutMs), owner);
            return MapConsumerWithToken(wrapper);
        }

        public void RemoveConsumer(Guid consumerId, string token)
        {
            var wrapper = _provider.GetItem(consumerId, token);
            _provider.RemoveItem(wrapper.Id);
        }

        public ICollection<TopicPartition> GetConsumerAssignment(Guid consumerId, string token)
        {
            var wrapper = _provider.GetItem(consumerId, token);
            wrapper.UpdateExpiration();
            return wrapper.GetAssignment();
        }

        public ICollection<TopicPartition> AssignConsumer(Guid consumerId, string token,
            IEnumerable<TopicPartitionOffset> request)
        {
            var wrapper = _provider.GetItem(consumerId, token);
            wrapper.UpdateExpiration();
            wrapper.Assign(request);
            return wrapper.GetAssignment();
        }

        public ConsumerMessage Consume(Guid consumerId, string token, int? timeoutMs)
        {
            var wrapper = _provider.GetItem(consumerId, token);
            wrapper.UpdateExpiration();
            return timeoutMs.HasValue
                ? wrapper.Consume(TimeSpan.FromMilliseconds(timeoutMs.Value))
                : wrapper.Consume();
        }

        public PartitionOffsets GetPartitionOffsets(Guid consumerId, string token, string topic, int partition,
            int? timeoutMs)
        {
            var wrapper = _provider.GetItem(consumerId, token);
            wrapper.UpdateExpiration();
            return timeoutMs.HasValue
                ? wrapper.QueryWatermarkOffsets(topic, partition, TimeSpan.FromMilliseconds(timeoutMs.Value))
                : wrapper.GetWatermarkOffsets(topic, partition);
        }

        private static Responses.Consumer MapConsumer(ConsumerWrapper source)
        {
            return new Responses.Consumer
            {
                Id = source.Id,
                Name = source.Name,
                User = source.User,
                KeyType = Enum.GetName(source.KeyType),
                ValueType = Enum.GetName(source.ValueType),
                ExpiresAt = source.ExpiresAt,
                Owner = source.Owner
            };
        }

        private static ConsumerWithToken MapConsumerWithToken(ConsumerWrapper source)
        {
            return new ConsumerWithToken
            {
                Id = source.Id,
                Name = source.Name,
                KeyType = Enum.GetName(source.KeyType),
                ValueType = Enum.GetName(source.ValueType),
                ExpiresAt = source.ExpiresAt,
                Owner = source.Owner,
                Token = source.Token
            };
        }
    }
}