using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace SergeSavel.KafkaRestProxy.Consumer.Requests
{
    public class AssignConsumerRequest
    {
        [Required] public Guid ConsumerId { get; init; }

        [Required] public IReadOnlyCollection<TopicPartitionOffset> Partitions { get; init; }

        public class TopicPartitionOffset
        {
            [Required] public string Topic { get; init; }

            [Required] public int Partition { get; init; }

            public long Offset { get; init; }
        }
    }
}