using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.Producer.Contract
{
    public class DeliveryResult
    {
        [Required] public string Status { get; init; }

        [Required] public int Topic { get; init; }

        [Required] public int Partition { get; init; }

        [Required] public long Offset { get; init; }

        [Required] public long Timestamp { get; init; }
    }
}