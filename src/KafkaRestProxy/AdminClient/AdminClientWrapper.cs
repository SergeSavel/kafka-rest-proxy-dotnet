﻿// Copyright 2021 Sergey Savelev
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

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using SergeSavel.KafkaRestProxy.AdminClient.Exceptions;
using SergeSavel.KafkaRestProxy.AdminClient.Responses;
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.Common.Exceptions;
using SergeSavel.KafkaRestProxy.Common.Mappers;
using KafkaException = Confluent.Kafka.KafkaException;
using Metadata = SergeSavel.KafkaRestProxy.Common.Responses.Metadata;

namespace SergeSavel.KafkaRestProxy.AdminClient;

public class AdminClientWrapper : ClientWrapper
{
    private readonly IAdminClient _adminClient;

    public AdminClientWrapper(string name, IDictionary<string, string> config, TimeSpan expirationTimeout) : base(
        name, config, expirationTimeout)
    {
        _adminClient = new AdminClientBuilder(config).Build();
    }

    public Metadata GetMetadata(TimeSpan timeout)
    {
        Confluent.Kafka.Metadata metadata;
        try
        {
            metadata = _adminClient.GetMetadata(timeout);
        }
        catch (KafkaException e)
        {
            throw new AdminClientException("Unable to get metadata.", e);
        }

        return new Metadata
        {
            Brokers = metadata.Brokers?.Select(CommonMapper.Map).ToList(),
            Topics = metadata.Topics?.Select(CommonMapper.Map).ToList(),
            OriginatingBrokerId = metadata.OriginatingBrokerId,
            OriginatingBrokerName = metadata.OriginatingBrokerName
        };
    }

    public Metadata GetMetadata(string topic, TimeSpan timeout)
    {
        Confluent.Kafka.Metadata metadata;
        try
        {
            metadata = _adminClient.GetMetadata(topic, timeout);
        }
        catch (KafkaException e)
        {
            throw new AdminClientException("Unable to get topic metadata.", e);
        }

        if (metadata.Topics[0].Error.Code == ErrorCode.UnknownTopicOrPart) throw new TopicNotFoundException(topic);
        return new Metadata
        {
            Brokers = metadata.Brokers?.Select(CommonMapper.Map).ToList(),
            Topics = metadata.Topics?.Select(CommonMapper.Map).ToList(),
            OriginatingBrokerId = metadata.OriginatingBrokerId,
            OriginatingBrokerName = metadata.OriginatingBrokerName
        };
    }

    public async Task CreateTopicAsync(string topic, int? numPartitions, short? replicationFactor,
        Dictionary<string, string> config, TimeSpan timeout)
    {
        var topicSpecification = new TopicSpecification
        {
            Name = topic,
            NumPartitions = numPartitions ?? -1,
            ReplicationFactor = replicationFactor ?? -1,
            Configs = config
        };

        var options = new CreateTopicsOptions
        {
            RequestTimeout = timeout
        };

        try
        {
            await _adminClient.CreateTopicsAsync(new[] { topicSpecification }, options);
        }
        catch (KafkaException e)
        {
            throw new AdminClientException("Unable to create topic.", e);
        }
    }

    public async Task<ResourceConfig> GetTopicConfigAsync(string topic, TimeSpan timeout)
    {
        var resource = new ConfigResource
        {
            Name = topic,
            Type = ResourceType.Topic
        };

        var options = new DescribeConfigsOptions
        {
            RequestTimeout = timeout
        };

        ICollection<DescribeConfigsResult> results;
        try
        {
            results = await _adminClient.DescribeConfigsAsync(new[] { resource }, options);
        }
        catch (KafkaException e)
        {
            throw new AdminClientException("Unable to get topic config.", e);
        }

        return Map(results.First());
    }

    public async Task<ResourceConfig> GetBrokerConfigAsync(int brokerId, TimeSpan timeout)
    {
        var resource = new ConfigResource
        {
            Name = brokerId.ToString(),
            Type = ResourceType.Broker
        };

        var options = new DescribeConfigsOptions
        {
            RequestTimeout = timeout
        };

        ICollection<DescribeConfigsResult> results;
        try
        {
            results = await _adminClient.DescribeConfigsAsync(new[] { resource }, options);
        }
        catch (KafkaException e)
        {
            throw new AdminClientException("Unable to get broker config.", e);
        }

        return Map(results.First());
    }

    private static ResourceConfig Map(DescribeConfigsResult source)
    {
        return new ResourceConfig
        {
            ResourceType = Enum.GetName(source.ConfigResource.Type),
            ResourceName = source.ConfigResource.Name,
            Entries = source.Entries?.ToDictionary(kv => kv.Value.Name, kv => new ResourceConfig.ConfigEntryValue
            {
                Value = kv.Value.Value,
                IsDefault = kv.Value.IsDefault,
                IsReadOnly = kv.Value.IsReadOnly,
                IsSensitive = kv.Value.IsSensitive
            })
        };
    }

    public override void Dispose()
    {
        _adminClient.Dispose();
    }
}