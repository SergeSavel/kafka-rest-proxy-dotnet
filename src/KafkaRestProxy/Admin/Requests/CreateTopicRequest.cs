using System.ComponentModel.DataAnnotations;

namespace SergeSavel.KafkaRestProxy.Admin.Requests
{
    public class CreateTopicRequest
    {
        [Required] public string Name { get; init; }
        [Range(1, 100000)] public int? NumPartitions { get; init; }
        public short? ReplicationFactor { get; init; }
    }
}