using pro.savel.KafkaRestProxy.Common.Exceptions;

namespace pro.savel.KafkaRestProxy.Consumer.Exceptions
{
    public class ConsumeException : KafkaException
    {
        public ConsumeException(Confluent.Kafka.KafkaException innerException) : base("Unable to receive message.",
            innerException)
        {
        }
    }
}