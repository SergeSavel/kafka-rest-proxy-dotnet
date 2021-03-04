namespace pro.savel.KafkaRestProxy.Consumer.Contract
{
    public class TopicPartition
    {
        public string Topic { get; init; }

        public int Partition { get; init; }
    }
}