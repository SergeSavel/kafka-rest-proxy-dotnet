using System.Linq;

namespace pro.savel.KafkaRestProxy.Entities
{
    public class TopicMetadata
    {
        public TopicMetadata()
        {
        }

        public TopicMetadata(Confluent.Kafka.TopicMetadata source)
        {
            Name = source.Topic;
            Partitions = source.Partitions.Select(p => new PartitionMetadata(p)).ToArray();
        }

        public string Name { get; init; }

        public PartitionMetadata[] Partitions { get; init; }

        public class PartitionMetadata
        {
            public PartitionMetadata()
            {
            }

            public PartitionMetadata(Confluent.Kafka.PartitionMetadata source)
            {
                Id = source.PartitionId;
                Leader = source.Leader;
                Replicas = source.Replicas;
                InSyncReplicas = source.InSyncReplicas;
            }

            public int Id { get; init; }

            public int Leader { get; init; }

            public int[] Replicas { get; init; }

            public int[] InSyncReplicas { get; init; }
        }
    }
}