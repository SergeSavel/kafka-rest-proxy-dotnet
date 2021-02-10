using System.Collections.Generic;

namespace pro.savel.KafkaRestProxy
{
    public class TopicInfo
    {
        public string Name { get; init; }

        public ICollection<PartitionInfo> Partitions { get; init; }

        public class PartitionInfo
        {
            public int Name { get; init; }

            public long BeginningOffset { get; init; }

            public long EndOffset { get; init; }
        }
    }
}