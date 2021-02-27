using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.AdminClient.Requests
{
    public class CreateTopicRequest
    {
        [Required] public string Name { get; init; }
        public int? NumPartitions { get; init; }
        public short? ReplicationFactor { get; init; }
    }
}