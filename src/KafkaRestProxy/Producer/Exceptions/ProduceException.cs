using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using pro.savel.KafkaRestProxy.Common.Exceptions;
using pro.savel.KafkaRestProxy.Common.Mappers;

namespace pro.savel.KafkaRestProxy.Producer.Exceptions
{
    public class ProduceException : HttpResponseException
    {
        public ProduceException(KafkaException innerException) : base("Unable to send message.", innerException)
        {
            StatusCode = StatusCodes.Status500InternalServerError;
            Value = CommonMapper.Map(innerException.Error);
        }
    }
}