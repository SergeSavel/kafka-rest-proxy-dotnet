namespace SergeSavel.KafkaRestProxy.AdminClient.Responses
{
    public abstract class MetadataBase
    {
        public int? OriginatingBrokerId { get; init; }

        public string OriginatingBrokerName { get; init; }
    }
}