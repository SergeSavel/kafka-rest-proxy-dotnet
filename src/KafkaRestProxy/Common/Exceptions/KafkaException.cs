using pro.savel.KafkaRestProxy.Common.Mappers;

namespace pro.savel.KafkaRestProxy.Common.Exceptions
{
    public class KafkaException : HttpResponseException
    {
        public KafkaException(string message, Confluent.Kafka.KafkaException innerException) : base(message,
            innerException)
        {
            StatusCode = 500;
            Value = CommonMapper.Map(innerException.Error);
        }
    }
}