using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using KafkaException = pro.savel.KafkaRestProxy.Common.Exceptions.KafkaException;

namespace pro.savel.KafkaRestProxy.Producer.Exceptions
{
    public class ProduceException : KafkaException
    {
        public ProduceException(Confluent.Kafka.KafkaException innerException) : base("Unable to send message.",
            innerException)
        {
            StatusCode = innerException.Error.Code switch
            {
                ErrorCode.UnknownTopicOrPart => StatusCodes.Status404NotFound,
                _ => StatusCodes.Status500InternalServerError
            };
        }
    }
}