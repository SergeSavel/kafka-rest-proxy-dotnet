using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace SergeSavel.KafkaRestProxy.Consumer.Requests
{
    public class CreateConsumerRequest
    {
        [Required] [Range(1000, 86400000)] public int ExpirationTimeoutMs { get; init; }

        public IReadOnlyDictionary<string, string> Config { get; init; }
    }
}