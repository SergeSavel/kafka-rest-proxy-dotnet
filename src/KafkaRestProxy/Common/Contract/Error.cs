namespace SergeSavel.KafkaRestProxy.Common.Contract
{
    public class Error
    {
        public int Code { get; init; }

        public string Reason { get; init; }

        public bool IsBrokerError { get; init; }

        public bool IsLocalError { get; init; }

        public bool IsFatal { get; init; }
    }
}