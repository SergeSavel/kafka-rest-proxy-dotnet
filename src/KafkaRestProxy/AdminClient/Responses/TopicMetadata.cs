using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.AdminClient.Responses
{
    public class TopicMetadata
    {
        [Required] public string Name { get; init; }

        [Required] public ICollection<PartitionMetadata> Partitions { get; init; }

        public int? OriginatingBrokerId { get; init; }

        public string OriginatingBrokerName { get; init; }

        public class PartitionMetadata
        {
            [Required] public int Id { get; init; }

            [Required] public int Leader { get; init; }

            [Required] public IEnumerable<int> Replicas { get; init; }

            [Required] public IEnumerable<int> InSyncReplicas { get; init; }
        }
    }
}