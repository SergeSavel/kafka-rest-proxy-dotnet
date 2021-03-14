﻿using System.Linq;
using Confluent.Kafka;
using pro.savel.KafkaRestProxy.AdminClient.Contract;
using BrokerMetadata = pro.savel.KafkaRestProxy.AdminClient.Contract.BrokerMetadata;
using Error = pro.savel.KafkaRestProxy.AdminClient.Contract.Error;
using Metadata = pro.savel.KafkaRestProxy.AdminClient.Contract.Metadata;
using TopicMetadata = pro.savel.KafkaRestProxy.AdminClient.Contract.TopicMetadata;

namespace pro.savel.KafkaRestProxy.AdminClient
{
    public static class AdminClientMapper
    {
        public static Metadata Map(Confluent.Kafka.Metadata source)
        {
            return new()
            {
                Brokers = source.Brokers.Select(Map).ToArray(),
                Topics = source.Topics.Select(Map).ToArray(),
                OriginatingBrokerId = source.OriginatingBrokerId,
                OriginatingBrokerName = source.OriginatingBrokerName
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

        public static TopicMetadata Map(Confluent.Kafka.TopicMetadata source)
        {
            return Map(source, null);
        }

        public static TopicMetadata Map(Confluent.Kafka.TopicMetadata source, Confluent.Kafka.Metadata metadata)
        {
            return new()
            {
                Topic = source.Topic,
                Partitions = source.Partitions.Select(Map).ToArray(),
                Error = source.Error.Code == ErrorCode.NoError ? null : Map(source.Error),
                OriginatingBrokerId = metadata?.OriginatingBrokerId,
                OriginatingBrokerName = metadata?.OriginatingBrokerName
            };
        }

        private static Error Map(Confluent.Kafka.Error source)
        {
            return new()
            {
                Code = (int) source.Code,
                Reason = source.Reason
            };
        }

        private static TopicMetadata.PartitionMetadata Map(PartitionMetadata source)
        {
            return new()
            {
                Id = source.PartitionId,
                Leader = source.Leader,
                Replicas = source.Replicas,
                InSyncReplicas = source.InSyncReplicas,
                Error = source.Error.Code == ErrorCode.NoError ? null : Map(source.Error)
            };
        }

        public static TopicsMetadata MapTopics(Confluent.Kafka.Metadata source)
        {
            return new()
            {
                Topics = source.Topics.Select(Map).ToArray(),
                OriginatingBrokerId = source.OriginatingBrokerId,
                OriginatingBrokerName = source.OriginatingBrokerName
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