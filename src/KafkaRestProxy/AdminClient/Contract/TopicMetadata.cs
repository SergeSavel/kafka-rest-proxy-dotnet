using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.AdminClient.Contract
{
    public class TopicMetadata
    {
        [Required] public string Name { get; init; }

        [Required] public PartitionMetadata[] Partitions { get; init; }

        public class PartitionMetadata
        {
            [Required] public int Id { get; init; }

            [Required] public int Leader { get; init; }

            [Required] public int[] Replicas { get; init; }

            [Required] public int[] InSyncReplicas { get; init; }
        }
    }
}