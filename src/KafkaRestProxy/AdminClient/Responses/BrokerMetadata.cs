using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.AdminClient.Responses
{
    public class BrokerMetadata
    {
        [Required] public int Id { get; init; }

        [Required] public string Host { get; init; }

        [Required] public int Port { get; init; }

        public int? OriginatingBrokerId { get; init; }

        public string OriginatingBrokerName { get; init; }
    }
}