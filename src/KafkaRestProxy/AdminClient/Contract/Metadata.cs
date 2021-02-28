using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.AdminClient.Contract
{
    public class Metadata
    {
        [Required] public ICollection<BrokerMetadata> Brokers { get; init; }

        [Required] public ICollection<TopicMetadata> Topics { get; init; }

        [Required] public int OriginatingBrokerId { get; init; }

        [Required] public string OriginatingBrokerName { get; init; }
    }
}