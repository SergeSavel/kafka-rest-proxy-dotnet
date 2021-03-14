using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.Consumer.Requests
{
    public class CreateConsumerRequest
    {
        [Required] [Range(1000, 86400000)] public int ExpirationTimeoutMs { get; init; }

        public string GroupId { get; init; }
    }
}