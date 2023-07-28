// Copyright 2023 Sergey Savelev
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Diagnostics;
using SergeSavel.KafkaRestProxy.Common.Responses;
using SergeSavel.KafkaRestProxy.Consumer.Requests;
using SergeSavel.KafkaRestProxy.Consumer.Responses;

namespace SergeSavel.KafkaRestProxy.Consumer;

public class ConsumerService
{
    private readonly ILogger<ConsumerService> _logger;
    private readonly ConsumerProvider _provider;

    public ConsumerService(ILogger<ConsumerService> logger, ConsumerProvider provider)
    {
        _logger = logger;
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
        _logger.LogDebug("Created consumer '{Id}' ('{Name}')", wrapper.Id, wrapper.Name);
        return MapConsumerWithToken(wrapper);
    }

    public void RemoveConsumer(Guid consumerId, string token)
    {
        var wrapper = _provider.GetItem(consumerId, token);
        _provider.RemoveItem(wrapper.Id);
        _logger.LogDebug("Removed consumer '{Id}' ('{Name}')", wrapper.Id, wrapper.Name);
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

    public ConsumerMessage Consume(Guid consumerId, string token, TimeSpan timeout)
    {
        var eventId = new EventId(Environment.CurrentManagedThreadId);

        var wrapper = _provider.GetItem(consumerId, token);
        wrapper.UpdateExpiration();

        _logger.LogDebug("Consume started ({Id})", wrapper.Id);

        var stopwatch = Stopwatch.StartNew();
        var result = wrapper.Consume(timeout);

        _logger.LogDebug(eventId, "Consume took {Ms} ms", stopwatch.ElapsedMilliseconds);

        return result;
    }

    public PartitionOffsets GetPartitionOffsets(Guid consumerId, string token, string topic, int partition,
        TimeSpan? timeout)
    {
        var wrapper = _provider.GetItem(consumerId, token);
        wrapper.UpdateExpiration();
        return timeout.HasValue
            ? wrapper.QueryWatermarkOffsets(topic, partition, timeout.Value)
            : wrapper.GetWatermarkOffsets(topic, partition);
    }

    public Metadata GetMetadata(Guid clientId, string token, TimeSpan timeout)
    {
        var wrapper = _provider.GetItem(clientId, token);
        wrapper.UpdateExpiration();
        return wrapper.GetMetadata(timeout);
    }

    public Metadata GetMetadata(Guid clientId, string token, string topic, TimeSpan timeout)
    {
        var wrapper = _provider.GetItem(clientId, token);
        wrapper.UpdateExpiration();
        return wrapper.GetMetadata(topic, timeout);
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
            Token = source.Token,
        };
    }
}