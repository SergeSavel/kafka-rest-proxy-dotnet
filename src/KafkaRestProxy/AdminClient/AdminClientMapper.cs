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
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using SergeSavel.KafkaRestProxy.AdminClient.Contract;
using SergeSavel.KafkaRestProxy.AdminClient.Responses;
using SergeSavel.KafkaRestProxy.Common.Mappers;
using BrokerMetadata = SergeSavel.KafkaRestProxy.AdminClient.Responses.BrokerMetadata;
using Metadata = SergeSavel.KafkaRestProxy.AdminClient.Responses.Metadata;
using TopicMetadata = SergeSavel.KafkaRestProxy.AdminClient.Responses.TopicMetadata;

namespace SergeSavel.KafkaRestProxy.AdminClient
{
    public static class AdminClientMapper
    {
        public static Metadata Map(Confluent.Kafka.Metadata source, bool verbose)
        {
            return new()
            {
                Brokers = source.Brokers.Select(Map).ToArray(),
                Topics = source.Topics.Select(topicMetadata => Map(topicMetadata, verbose)).ToArray(),
                OriginatingBrokerId = verbose ? source.OriginatingBrokerId : null,
                OriginatingBrokerName = verbose ? source.OriginatingBrokerName : null
            };
        }

        public static BrokerMetadata Map(Confluent.Kafka.BrokerMetadata source)
        {
            return Map(source, null);
        }

        public static BrokerMetadata Map(Confluent.Kafka.BrokerMetadata source, Confluent.Kafka.Metadata metadata)
        {
            return new()
            {
                Id = source.BrokerId,
                Host = source.Host,
                Port = source.Port,
                OriginatingBrokerId = metadata?.OriginatingBrokerId,
                OriginatingBrokerName = metadata?.OriginatingBrokerName
            };
        }

        public static TopicMetadata Map(Confluent.Kafka.TopicMetadata source, bool verbose)
        {
            return Map(source, null, verbose);
        }

        public static TopicMetadata Map(Confluent.Kafka.TopicMetadata source, Confluent.Kafka.Metadata metadata,
            bool verbose)
        {
            return new()
            {
                Topic = source.Topic,
                Partitions = source.Partitions.Select(partitionMetadata => Map(partitionMetadata, verbose)).ToArray(),
                OriginatingBrokerId = verbose ? metadata?.OriginatingBrokerId : null,
                OriginatingBrokerName = verbose ? metadata?.OriginatingBrokerName : null,
                Error = CommonMapper.Map(source.Error)
            };
        }

        private static TopicMetadata.PartitionMetadata Map(PartitionMetadata source, bool verbose)
        {
            return new()
            {
                Partition = source.PartitionId,
                Leader = verbose ? source.Leader : null,
                Replicas = verbose ? source.Replicas : null,
                InSyncReplicas = verbose ? source.InSyncReplicas : null,
                Error = CommonMapper.Map(source.Error)
            };
        }

        public static ResourceConfig Map(DescribeConfigsResult source)
        {
            return new()
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

        public static TopicsMetadata MapTopics(Confluent.Kafka.Metadata source, bool verbose)
        {
            return new()
            {
                Topics = source.Topics.Select(topicMetadata => Map(topicMetadata, verbose)).ToArray(),
                OriginatingBrokerId = verbose ? source.OriginatingBrokerId : null,
                OriginatingBrokerName = verbose ? source.OriginatingBrokerName : null
            };
        }

        public static BrokersMetadata MapBrokers(Confluent.Kafka.Metadata source)
        {
            return new()
            {
                Brokers = source.Brokers.Select(Map).ToArray(),
                OriginatingBrokerId = source.OriginatingBrokerId,
                OriginatingBrokerName = source.OriginatingBrokerName
            };
        }
    }
}