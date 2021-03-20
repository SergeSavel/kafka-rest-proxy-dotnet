using Microsoft.AspNetCore.Http;
using pro.savel.KafkaRestProxy.Common.Exceptions;

namespace pro.savel.KafkaRestProxy.AdminClient.Exceptions
{
    public class TopicAlreadyExistsException : HttpResponseException
    {
        public TopicAlreadyExistsException(string topic)
        {
            StatusCode = StatusCodes.Status400BadRequest;
            Value = $"Topic '{topic}' already exists.";
        }
    }
}