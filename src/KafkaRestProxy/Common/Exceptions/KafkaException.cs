using SergeSavel.KafkaRestProxy.Common.Mappers;

namespace SergeSavel.KafkaRestProxy.Common.Exceptions
{
    public class KafkaException : HttpResponseException
    {
        protected KafkaException(string message, Confluent.Kafka.KafkaException innerException) : base(message,
            innerException)
        {
            StatusCode = 500;
            Value = CommonMapper.Map(innerException.Error);
        }
    }
}