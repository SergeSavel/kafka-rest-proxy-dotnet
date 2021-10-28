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
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using SergeSavel.KafkaRestProxy.AdminClient.Exceptions;
using SergeSavel.KafkaRestProxy.AdminClient.Responses;
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.Common.Mappers;
using BrokerMetadata = SergeSavel.KafkaRestProxy.AdminClient.Responses.BrokerMetadata;
using Metadata = SergeSavel.KafkaRestProxy.AdminClient.Responses.Metadata;
using TopicMetadata = SergeSavel.KafkaRestProxy.AdminClient.Responses.TopicMetadata;

namespace SergeSavel.KafkaRestProxy.AdminClient
{
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
            var metadata = _adminClient.GetMetadata(timeout);
            return new Metadata
            {
                Brokers = metadata.Brokers.Select(brokerMetadata => Map(brokerMetadata)).ToArray(),
                Topics = metadata.Topics.Select(topicMetadata => Map(topicMetadata)).ToArray(),
                OriginatingBrokerId = metadata.OriginatingBrokerId,
                OriginatingBrokerName = metadata.OriginatingBrokerName
            };
        }

        public TopicsMetadata GetTopicsMetadata(TimeSpan timeout)
        {
            var metadata = _adminClient.GetMetadata(timeout);
            return new TopicsMetadata
            {
                Topics = metadata.Topics.Select(topicMetadata => Map(topicMetadata)).ToArray(),
                OriginatingBrokerId = metadata.OriginatingBrokerId,
                OriginatingBrokerName = metadata.OriginatingBrokerName
            };
        }

        public TopicMetadata GetTopicMetadata(string topic, TimeSpan timeout)
        {
            var metadata = _adminClient.GetMetadata(topic, timeout);

            var topicMetadata = metadata.Topics[0];

            if (topicMetadata.Error.Code == ErrorCode.UnknownTopicOrPart)
                throw new TopicNotFoundException(topic);

            return Map(topicMetadata, metadata);
        }

        public BrokersMetadata GetBrokersMetadata(TimeSpan timeout)
        {
            var metadata = _adminClient.GetMetadata(timeout);
            return new BrokersMetadata
            {
                Brokers = metadata.Brokers.Select(brokerMetadata => Map(brokerMetadata)).ToArray(),
                OriginatingBrokerId = metadata.OriginatingBrokerId,
                OriginatingBrokerName = metadata.OriginatingBrokerName
            };
        }

        public BrokerMetadata GetBrokerMetadata(int brokerId, TimeSpan timeout)
        {
            var metadata = _adminClient.GetMetadata(timeout);
            var brokerMetadata = metadata.Brokers.Find(brokerMetadata => brokerMetadata.BrokerId == brokerId);
            if (brokerMetadata == null) throw new BrokerNotFoundException(brokerId);
            return Map(brokerMetadata, metadata);
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

        private static BrokerMetadata Map(Confluent.Kafka.BrokerMetadata source,
            Confluent.Kafka.Metadata metadata = null)
        {
            return new BrokerMetadata
            {
                Id = source.BrokerId,
                Host = source.Host,
                Port = source.Port,
                OriginatingBrokerId = metadata?.OriginatingBrokerId,
                OriginatingBrokerName = metadata?.OriginatingBrokerName
            };
        }

        private static TopicMetadata Map(Confluent.Kafka.TopicMetadata source, Confluent.Kafka.Metadata metadata = null)
        {
            return new TopicMetadata
            {
                Topic = source.Topic,
                Partitions = source.Partitions.Select(Map).ToArray(),
                Error = CommonMapper.Map(source.Error),
                OriginatingBrokerId = metadata?.OriginatingBrokerId,
                OriginatingBrokerName = metadata?.OriginatingBrokerName
            };
        }

        private static TopicMetadata.PartitionMetadata Map(PartitionMetadata source)
        {
            return new TopicMetadata.PartitionMetadata
            {
                Partition = source.PartitionId,
                Leader = source.Leader,
                Replicas = source.Replicas,
                InSyncReplicas = source.InSyncReplicas,
                Error = CommonMapper.Map(source.Error)
            };
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
}