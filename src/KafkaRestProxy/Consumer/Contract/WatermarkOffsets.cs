namespace pro.savel.KafkaRestProxy.Consumer.Contract
{
    public class WatermarkOffsets
    {
        public string Topic { get; init; }

        public int Partition { get; init; }

        public long Low { get; init; }

        public long High { get; init; }
    }
}