using System.Linq;
using pro.savel.KafkaRestProxy.AdminClient.Contract;

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
            return new()
            {
                Id = source.BrokerId,
                Host = source.Host,
                Port = source.Port
            };
        }

        public static TopicMetadata Map(Confluent.Kafka.TopicMetadata source)
        {
            return new()
            {
                Name = source.Topic,
                Partitions = source.Partitions.Select(Map).ToArray()
            };
        }

        private static TopicMetadata.PartitionMetadata Map(Confluent.Kafka.PartitionMetadata source)
        {
            return new()
            {
                Id = source.PartitionId,
                Leader = source.Leader,
                Replicas = source.Replicas,
                InSyncReplicas = source.InSyncReplicas
            };
        }
    }
}