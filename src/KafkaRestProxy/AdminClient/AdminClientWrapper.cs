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

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using SergeSavel.KafkaRestProxy.AdminClient.Exceptions;
using SergeSavel.KafkaRestProxy.AdminClient.Responses;
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.Common.Exceptions;
using SergeSavel.KafkaRestProxy.Common.Mappers;
using AclBinding = SergeSavel.KafkaRestProxy.AdminClient.Contract.AclBinding;
using AclBindingFilter = SergeSavel.KafkaRestProxy.AdminClient.Contract.AclBindingFilter;
using DeleteAclsResult = SergeSavel.KafkaRestProxy.AdminClient.Contract.DeleteAclsResult;
using KafkaException = Confluent.Kafka.KafkaException;
using Metadata = SergeSavel.KafkaRestProxy.Common.Responses.Metadata;

namespace SergeSavel.KafkaRestProxy.AdminClient;

public class AdminClientWrapper : ClientWrapper
{
    private readonly IAdminClient _adminClient;

    public AdminClientWrapper(string name, IDictionary<string, string> config, TimeSpan expirationTimeout) : base(
        name, config, expirationTimeout)
    {
        try
        {
            _adminClient = new AdminClientBuilder(config).Build();
        }
        catch (Exception e)
        {
            throw new ClientConfigException(e);
        }
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
            await _adminClient.CreateTopicsAsync(new[] { topicSpecification }, options).ConfigureAwait(false);
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
            results = await _adminClient.DescribeConfigsAsync(new[] { resource }, options).ConfigureAwait(false);
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
            results = await _adminClient.DescribeConfigsAsync(new[] { resource }, options).ConfigureAwait(false);
        }
        catch (KafkaException e)
        {
            throw new AdminClientException("Unable to get broker config.", e);
        }

        return Map(results.First());
    }

    public async Task CreateAclsAsync(IEnumerable<AclBinding> aclBindings, TimeSpan timeout)
    {
        var outerAclBindings = aclBindings.Select(Map);

        var options = new CreateAclsOptions
        {
            RequestTimeout = timeout
        };

        try
        {
            await _adminClient.CreateAclsAsync(outerAclBindings, options).ConfigureAwait(false);
        }
        catch (KafkaException e)
        {
            throw new AdminClientException("Unable to create ACL bindings.", e);
        }
    }

    public async Task<List<AclBinding>> DescribeAclsAsync(AclBindingFilter aclBindingFilter,
        TimeSpan timeout)
    {
        var outerFilter = Map(aclBindingFilter);

        var options = new DescribeAclsOptions
        {
            RequestTimeout = timeout
        };

        DescribeAclsResult result;
        try
        {
            result = await _adminClient.DescribeAclsAsync(outerFilter, options).ConfigureAwait(false);
        }
        catch (KafkaException e)
        {
            throw new AdminClientException("Unable to get ACL bindings.", e);
        }

        return result.AclBindings.Select(Map).ToList();
    }

    public async Task<List<DeleteAclsResult>> DeleteAclsAsync(
        IEnumerable<AclBindingFilter> aclBindingFilters, TimeSpan timeout)
    {
        var outerFilters = aclBindingFilters.Select(Map);

        var options = new DeleteAclsOptions
        {
            RequestTimeout = timeout
        };

        ICollection<Confluent.Kafka.Admin.DeleteAclsResult> results;
        try
        {
            results = await _adminClient.DeleteAclsAsync(outerFilters, options).ConfigureAwait(false);
        }
        catch (KafkaException e)
        {
            throw new AdminClientException("Unable to get ACL bindings.", e);
        }

        return results
            .Select(outerResult => new DeleteAclsResult
                { AclBindings = outerResult.AclBindings.Select(Map).ToList() })
            .ToList();
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

    private static Confluent.Kafka.Admin.AclBinding Map(AclBinding source)
    {
        return new Confluent.Kafka.Admin.AclBinding
        {
            Pattern = Map(source.Pattern),
            Entry = Map(source.Entry)
        };
    }

    private static AclBinding Map(Confluent.Kafka.Admin.AclBinding source)
    {
        return new AclBinding
        {
            Pattern = Map(source.Pattern),
            Entry = Map(source.Entry)
        };
    }

    private static Confluent.Kafka.Admin.AclBindingFilter Map(AclBindingFilter source)
    {
        return new Confluent.Kafka.Admin.AclBindingFilter
        {
            PatternFilter = Map(source.PatternFilter),
            EntryFilter = Map(source.EntryFilter)
        };
    }

    private static ResourcePattern Map(Contract.ResourcePattern source)
    {
        return new ResourcePattern
        {
            Type = Map(source.ResourceType),
            Name = source.Name,
            ResourcePatternType = Map(source.PatternType)
        };
    }

    private static Contract.ResourcePattern Map(ResourcePattern source)
    {
        return new Contract.ResourcePattern
        {
            ResourceType = Map(source.Type),
            Name = source.Name,
            PatternType = Map(source.ResourcePatternType)
        };
    }

    private static ResourcePatternFilter Map(Contract.ResourcePatternFilter source)
    {
        return new ResourcePatternFilter
        {
            Type = Map(source.ResourceType),
            Name = source.Name,
            ResourcePatternType = Map(source.PatternType)
        };
    }

    private static AccessControlEntry Map(Contract.AccessControlEntry source)
    {
        return new AccessControlEntry
        {
            Principal = source.Principal,
            Host = source.Host,
            Operation = Map(source.Operation),
            PermissionType = Map(source.Permission)
        };
    }

    private static Contract.AccessControlEntry Map(AccessControlEntry source)
    {
        return new Contract.AccessControlEntry
        {
            Principal = source.Principal,
            Host = source.Host,
            Operation = Map(source.Operation),
            Permission = Map(source.PermissionType)
        };
    }

    private static AccessControlEntryFilter Map(Contract.AccessControlEntryFilter source)
    {
        return new AccessControlEntryFilter
        {
            Principal = source.Principal,
            Host = source.Host,
            Operation = Map(source.Operation),
            PermissionType = Map(source.Permission)
        };
    }

    private static ResourceType Map(Contract.ResourceType source)
    {
        return source switch
        {
            Contract.ResourceType.Unknown => ResourceType.Unknown,
            Contract.ResourceType.Any => ResourceType.Any,
            Contract.ResourceType.Topic => ResourceType.Topic,
            Contract.ResourceType.Group => ResourceType.Group,
            Contract.ResourceType.Broker => ResourceType.Broker,
            _ => throw new ArgumentException($"Unexpected inner resource type: {source}", nameof(source))
        };
    }

    private static Contract.ResourceType Map(ResourceType source)
    {
        return source switch
        {
            ResourceType.Unknown => Contract.ResourceType.Unknown,
            ResourceType.Any => Contract.ResourceType.Any,
            ResourceType.Topic => Contract.ResourceType.Topic,
            ResourceType.Group => Contract.ResourceType.Group,
            ResourceType.Broker => Contract.ResourceType.Broker,
            _ => throw new ArgumentException($"Unexpected outer resource type: {source}", nameof(source))
        };
    }

    private static ResourcePatternType Map(Contract.ResourcePatternType source)
    {
        return source switch
        {
            Contract.ResourcePatternType.Unknown => ResourcePatternType.Unknown,
            Contract.ResourcePatternType.Any => ResourcePatternType.Any,
            Contract.ResourcePatternType.Match => ResourcePatternType.Match,
            Contract.ResourcePatternType.Literal => ResourcePatternType.Literal,
            Contract.ResourcePatternType.Prefixed => ResourcePatternType.Prefixed,
            _ => throw new ArgumentException($"Unexpected inner resource pattern type: {source}", nameof(source))
        };
    }

    private static Contract.ResourcePatternType Map(ResourcePatternType source)
    {
        return source switch
        {
            ResourcePatternType.Unknown => Contract.ResourcePatternType.Unknown,
            ResourcePatternType.Any => Contract.ResourcePatternType.Any,
            ResourcePatternType.Match => Contract.ResourcePatternType.Match,
            ResourcePatternType.Literal => Contract.ResourcePatternType.Literal,
            ResourcePatternType.Prefixed => Contract.ResourcePatternType.Prefixed,
            _ => throw new ArgumentException($"Unexpected outer resource pattern type: {source}", nameof(source))
        };
    }

    private static AclOperation Map(Contract.AclOperation source)
    {
        return source switch
        {
            Contract.AclOperation.Unknown => AclOperation.Unknown,
            Contract.AclOperation.Any => AclOperation.Any,
            Contract.AclOperation.All => AclOperation.All,
            Contract.AclOperation.Read => AclOperation.Read,
            Contract.AclOperation.Write => AclOperation.Write,
            Contract.AclOperation.Create => AclOperation.Create,
            Contract.AclOperation.Delete => AclOperation.Delete,
            Contract.AclOperation.Alter => AclOperation.Alter,
            Contract.AclOperation.Describe => AclOperation.Describe,
            Contract.AclOperation.ClusterAction => AclOperation.ClusterAction,
            Contract.AclOperation.DescribeConfigs => AclOperation.DescribeConfigs,
            Contract.AclOperation.AlterConfigs => AclOperation.AlterConfigs,
            Contract.AclOperation.IdempotentWrite => AclOperation.IdempotentWrite,
            _ => throw new ArgumentException($"Unexpected inner ACl operation: {source}", nameof(source))
        };
    }

    private static Contract.AclOperation Map(AclOperation source)
    {
        return source switch
        {
            AclOperation.Unknown => Contract.AclOperation.Unknown,
            AclOperation.Any => Contract.AclOperation.Any,
            AclOperation.All => Contract.AclOperation.All,
            AclOperation.Read => Contract.AclOperation.Read,
            AclOperation.Write => Contract.AclOperation.Write,
            AclOperation.Create => Contract.AclOperation.Create,
            AclOperation.Delete => Contract.AclOperation.Delete,
            AclOperation.Alter => Contract.AclOperation.Alter,
            AclOperation.Describe => Contract.AclOperation.Describe,
            AclOperation.ClusterAction => Contract.AclOperation.ClusterAction,
            AclOperation.DescribeConfigs => Contract.AclOperation.DescribeConfigs,
            AclOperation.AlterConfigs => Contract.AclOperation.AlterConfigs,
            AclOperation.IdempotentWrite => Contract.AclOperation.IdempotentWrite,
            _ => throw new ArgumentException($"Unexpected outer ACl operation: {source}", nameof(source))
        };
    }

    private static AclPermissionType Map(Contract.AclPermissionType source)
    {
        return source switch
        {
            Contract.AclPermissionType.Unknown => AclPermissionType.Unknown,
            Contract.AclPermissionType.Any => AclPermissionType.Any,
            Contract.AclPermissionType.Deny => AclPermissionType.Deny,
            Contract.AclPermissionType.Allow => AclPermissionType.Allow,
            _ => throw new ArgumentException($"Unexpected inner ACl permission type: {source}", nameof(source))
        };
    }

    private static Contract.AclPermissionType Map(AclPermissionType source)
    {
        return source switch
        {
            AclPermissionType.Unknown => Contract.AclPermissionType.Unknown,
            AclPermissionType.Any => Contract.AclPermissionType.Any,
            AclPermissionType.Deny => Contract.AclPermissionType.Deny,
            AclPermissionType.Allow => Contract.AclPermissionType.Allow,
            _ => throw new ArgumentException($"Unexpected outer ACl permission type: {source}", nameof(source))
        };
    }

    public override void Dispose()
    {
        _adminClient.Dispose();
    }
}