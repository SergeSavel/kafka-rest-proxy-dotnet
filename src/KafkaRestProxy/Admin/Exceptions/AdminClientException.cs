using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using KafkaException = SergeSavel.KafkaRestProxy.Common.Exceptions.KafkaException;

namespace SergeSavel.KafkaRestProxy.Admin.Exceptions
{
    public class AdminClientException : KafkaException
    {
        public AdminClientException(string message, Confluent.Kafka.KafkaException innerException) : base(message,
            innerException)
        {
            StatusCode = innerException.Error.Code switch
            {
                ErrorCode.UnknownTopicOrPart => StatusCodes.Status400BadRequest,
                ErrorCode.TopicAlreadyExists => StatusCodes.Status400BadRequest,
                _ => StatusCodes.Status500InternalServerError
            };
        }
    }
}