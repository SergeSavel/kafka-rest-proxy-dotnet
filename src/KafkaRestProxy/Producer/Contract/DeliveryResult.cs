using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.Producer.Contract
{
    public class DeliveryResult
    {
        public enum PersistenceStatus
        {
            NotPersisted,
            Persisted,
            PossiblyPersisted
        }

        [Required] public PersistenceStatus Status { get; init; }

        [Required] public int PartitionId { get; init; }

        [Required] public long Offset { get; init; }

        [Required] public long Timestamp { get; init; }
    }
}