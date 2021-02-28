﻿using System.Linq;
using Confluent.Kafka;
using pro.savel.KafkaRestProxy.AdminClient.Responses;
using BrokerMetadata = pro.savel.KafkaRestProxy.AdminClient.Responses.BrokerMetadata;
using Metadata = pro.savel.KafkaRestProxy.AdminClient.Responses.Metadata;
using TopicMetadata = pro.savel.KafkaRestProxy.AdminClient.Responses.TopicMetadata;

namespace pro.savel.KafkaRestProxy.AdminClient
{
    public static class AdminClientMapper
    {
        public static Metadata Map(Confluent.Kafka.Metadata source)
        {
            return new()
            {
                Brokers = source.Brokers.Select(Map).ToList(),
                Topics = source.Topics.Select(Map).ToList(),
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
                Name = source.Topic,
                Partitions = source.Partitions.Select(Map).ToList(),
                OriginatingBrokerId = metadata?.OriginatingBrokerId,
                OriginatingBrokerName = metadata?.OriginatingBrokerName
            };
        }

        private static TopicMetadata.PartitionMetadata Map(PartitionMetadata source)
        {
            return new()
            {
                Id = source.PartitionId,
                Leader = source.Leader,
                Replicas = source.Replicas,
                InSyncReplicas = source.InSyncReplicas
            };
        }

        public static TopicsMetadata MapTopics(Confluent.Kafka.Metadata source)
        {
            return new()
            {
                Topics = source.Topics.Select(Map).ToList(),
                OriginatingBrokerId = source.OriginatingBrokerId,
                OriginatingBrokerName = source.OriginatingBrokerName
            };
        }

        public static BrokersMetadata MapBrokers(Confluent.Kafka.Metadata source)
        {
            return new()
            {
                Brokers = source.Brokers.Select(Map).ToList(),
                OriginatingBrokerId = source.OriginatingBrokerId,
                OriginatingBrokerName = source.OriginatingBrokerName
            };
        }
    }
}