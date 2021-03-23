using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using KafkaException = SergeSavel.KafkaRestProxy.Common.Exceptions.KafkaException;

namespace SergeSavel.KafkaRestProxy.Producer.Exceptions
{
    public class ProduceException : KafkaException
    {
        public ProduceException(Confluent.Kafka.KafkaException innerException) : base("Unable to send message.",
            innerException)
        {
            StatusCode = innerException.Error.Code switch
            {
                ErrorCode.UnknownTopicOrPart => StatusCodes.Status400BadRequest,
                ErrorCode.MsgSizeTooLarge => StatusCodes.Status400BadRequest,
                _ => StatusCodes.Status500InternalServerError
            };
        }
    }
}