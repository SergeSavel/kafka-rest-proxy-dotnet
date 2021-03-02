using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.AdminClient.Contract
{
    public class TopicMetadata
    {
        [Required] public string Topic { get; init; }

        [Required] public ICollection<PartitionMetadata> Partitions { get; init; }

        public Error Error { get; init; }

        public int? OriginatingBrokerId { get; init; }

        public string OriginatingBrokerName { get; init; }

        public class PartitionMetadata
        {
            [Required] public int Id { get; init; }

            [Required] public int Leader { get; init; }

            [Required] public ICollection<int> Replicas { get; init; }

            [Required] public ICollection<int> InSyncReplicas { get; init; }

            public Error Error { get; init; }
        }
    }
}