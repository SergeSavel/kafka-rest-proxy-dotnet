namespace pro.savel.KafkaRestProxy.Producer.Contract
{
    public class DeliveryResult
    {
        public string Status { get; init; }

        public string Topic { get; init; }

        public int Partition { get; init; }

        public long Offset { get; init; }

        public long Timestamp { get; init; }
    }
}