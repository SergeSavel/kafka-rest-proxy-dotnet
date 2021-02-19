namespace pro.savel.KafkaRestProxy.AdminClient.Contract
{
    public class TopicMetadata
    {
        public string Name { get; init; }
        public PartitionMetadata[] Partitions { get; init; }

        public class PartitionMetadata
        {
            public int Id { get; init; }
            public int Leader { get; init; }
            public int[] Replicas { get; init; }
            public int[] InSyncReplicas { get; init; }
        }
    }
}