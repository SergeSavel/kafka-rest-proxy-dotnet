namespace pro.savel.KafkaRestProxy.AdminClient.Contract
{
    public class BrokerMetadata
    {
        public int Id { get; init; }
        public string Host { get; init; }
        public int Port { get; init; }
    }
}