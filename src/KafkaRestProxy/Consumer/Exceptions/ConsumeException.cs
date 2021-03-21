using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.Consumer.Exceptions
{
    public class ConsumeException : KafkaException
    {
        public ConsumeException(string message, Confluent.Kafka.KafkaException innerException) : base(message,
            innerException)
        {
        }
    }
}