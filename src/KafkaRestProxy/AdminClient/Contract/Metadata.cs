namespace pro.savel.KafkaRestProxy.AdminClient.Contract
{
    public class Metadata
    {
        public BrokerMetadata[] Brokers { get; init; }
        public TopicMetadata[] Topics { get; init; }
        public int OriginatingBrokerId { get; init; }
        public string OriginatingBrokerName { get; init; }
    }
}