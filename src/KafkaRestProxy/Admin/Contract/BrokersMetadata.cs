using System.Collections.Generic;

namespace SergeSavel.KafkaRestProxy.Admin.Contract
{
    public class BrokersMetadata
    {
        public ICollection<BrokerMetadata> Brokers { get; init; }

        public int OriginatingBrokerId { get; init; }

        public string OriginatingBrokerName { get; init; }
    }
}