namespace SergeSavel.KafkaRestProxy.Consumer.Contract
{
    public class WatermarkOffsets
    {
        public string Topic { get; init; }

        public int Partition { get; init; }

        public long Low { get; init; }

        public long High { get; init; }

        public long? Current { get; init; }
    }
}