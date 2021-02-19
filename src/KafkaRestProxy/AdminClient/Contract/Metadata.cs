using System.Linq;

namespace pro.savel.KafkaRestProxy.AdminClient.Contract
{
    public class Metadata
    {
        public Metadata()
        {
        }

        public Metadata(Confluent.Kafka.Metadata source)
        {
            Brokers = source.Brokers.Select(b => new BrokerMetadata(b)).ToArray();
            Topics = source.Topics.Select(b => new TopicMetadata(b)).ToArray();
            OriginatingBrokerId = source.OriginatingBrokerId;
            OriginatingBrokerName = source.OriginatingBrokerName;
        }

        public BrokerMetadata[] Brokers { get; init; }

        public TopicMetadata[] Topics { get; init; }

        public int OriginatingBrokerId { get; init; }

        public string OriginatingBrokerName { get; init; }
    }
}