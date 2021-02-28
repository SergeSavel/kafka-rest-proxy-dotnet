using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.AdminClient.Responses
{
    public class BrokersMetadata
    {
        [Required] public ICollection<BrokerMetadata> Brokers { get; init; }

        [Required] public int OriginatingBrokerId { get; init; }

        [Required] public string OriginatingBrokerName { get; init; }
    }
}