using System;

namespace SergeSavel.KafkaRestProxy.Consumer.Contract
{
    public class Consumer
    {
        public Guid Id { get; init; }

        public DateTime ExpiresAt { get; init; }
    }
}