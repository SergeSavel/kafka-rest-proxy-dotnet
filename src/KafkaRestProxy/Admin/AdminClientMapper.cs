using System;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using SergeSavel.KafkaRestProxy.Admin.Contract;
using SergeSavel.KafkaRestProxy.Common.Mappers;
using BrokerMetadata = SergeSavel.KafkaRestProxy.Admin.Contract.BrokerMetadata;
using Metadata = SergeSavel.KafkaRestProxy.Admin.Contract.Metadata;
using TopicMetadata = SergeSavel.KafkaRestProxy.Admin.Contract.TopicMetadata;

namespace SergeSavel.KafkaRestProxy.Admin
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