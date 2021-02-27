using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.Consumer.Requests
{
    public class CreateConsumerRequest
    {
        [Required] [Range(0, 86400000)] public int ExpirationTimeoutMs;

        [Required] public string GroupId;

        [Required] public string Topic { get; init; }

        [Required] public int Partition { get; init; }

        public long? Position { get; init; }
    }
}