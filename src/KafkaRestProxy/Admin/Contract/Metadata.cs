using System.Collections.Generic;

namespace SergeSavel.KafkaRestProxy.Admin.Contract
{
    public class Metadata
    {
        public ICollection<BrokerMetadata> Brokers { get; init; }

        public ICollection<TopicMetadata> Topics { get; init; }

        public int OriginatingBrokerId { get; init; }

        public string OriginatingBrokerName { get; init; }
    }
}