using System;
using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.Consumer.Contract
{
    public class Consumer
    {
        public Guid Id { get; init; }
        [Required] public string Topic { get; init; }
        [Required] public int Partition { get; init; }
        [Required] public long Position { get; init; }
        public long BeginningOffset { get; init; }
        public long EndOffset { get; init; }
        public DateTime ExpiresAt { get; init; }
    }
}