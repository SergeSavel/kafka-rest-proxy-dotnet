using System.Collections.Generic;

namespace pro.savel.KafkaRestProxy.AdminClient.Contract
{
    public class TopicsMetadata
    {
        public ICollection<TopicMetadata> Topics { get; init; }

        public int OriginatingBrokerId { get; init; }

        public string OriginatingBrokerName { get; init; }
    }
}