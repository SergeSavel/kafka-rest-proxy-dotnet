using System.Collections.Generic;
using pro.savel.KafkaRestProxy.Common.Contract;

namespace pro.savel.KafkaRestProxy.AdminClient.Contract
{
    public class TopicMetadata
    {
        public string Topic { get; init; }

        public ICollection<PartitionMetadata> Partitions { get; init; }

        public Error Error { get; init; }

        public int? OriginatingBrokerId { get; init; }

        public string OriginatingBrokerName { get; init; }

        public class PartitionMetadata
        {
            public int Id { get; init; }

            public int Leader { get; init; }

            public ICollection<int> Replicas { get; init; }

            public ICollection<int> InSyncReplicas { get; init; }

            public Error Error { get; init; }
        }
    }
}