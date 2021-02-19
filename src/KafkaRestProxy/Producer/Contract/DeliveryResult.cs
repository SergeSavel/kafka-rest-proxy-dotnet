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

        public PersistenceStatus Status { get; init; }
        public int PartitionId { get; init; }
        public long Offset { get; init; }
        public long Timestamp { get; init; }
    }
}