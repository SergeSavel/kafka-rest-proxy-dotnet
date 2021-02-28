using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.AdminClient.Contract
{
    public class TopicsMetadata
    {
        [Required] public ICollection<TopicMetadata> Topics { get; init; }

        [Required] public int OriginatingBrokerId { get; init; }

        [Required] public string OriginatingBrokerName { get; init; }
    }
}