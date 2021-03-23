namespace SergeSavel.KafkaRestProxy.Consumer.Contract
{
    public class PartitionOffsets
    {
        public string Topic { get; init; }

        public int Partition { get; init; }

        public long Low { get; init; }

        public long High { get; init; }

        public Offset Current { get; init; }

        public class Offset
        {
            public long Value { get; init; }

            public bool IsSpecial { get; init; }

            public string SpecialValue { get; init; }
        }
    }
}