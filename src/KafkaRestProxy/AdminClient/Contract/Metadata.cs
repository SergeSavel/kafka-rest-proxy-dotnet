using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.AdminClient.Contract
{
    public class Metadata
    {
        [Required] public BrokerMetadata[] Brokers { get; init; }

        [Required] public TopicMetadata[] Topics { get; init; }

        [Required] public int OriginatingBrokerId { get; init; }

        [Required] public string OriginatingBrokerName { get; init; }
    }
}