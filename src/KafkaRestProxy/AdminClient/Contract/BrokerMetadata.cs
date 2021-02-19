namespace pro.savel.KafkaRestProxy.AdminClient.Contract
{
    public class BrokerMetadata
    {
        public BrokerMetadata()
        {
        }

        public BrokerMetadata(Confluent.Kafka.BrokerMetadata source)
        {
            Id = source.BrokerId;
            Host = source.Host;
            Port = source.Port;
        }

        public int Id { get; init; }

        public string Host { get; init; }

        public int Port { get; init; }
    }
}