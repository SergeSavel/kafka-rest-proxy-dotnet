using pro.savel.KafkaRestProxy.Common.Exceptions;

namespace pro.savel.KafkaRestProxy.Consumer.Exceptions
{
    public class ConsumeException : KafkaException
    {
        public ConsumeException(string message, Confluent.Kafka.KafkaException innerException) : base(message,
            innerException)
        {
        }
    }
}