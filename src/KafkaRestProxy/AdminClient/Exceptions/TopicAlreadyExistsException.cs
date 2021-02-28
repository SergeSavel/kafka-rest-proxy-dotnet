using Microsoft.AspNetCore.Http;
using pro.savel.KafkaRestProxy.Common;

namespace pro.savel.KafkaRestProxy.AdminClient.Exceptions
{
    public class TopicAlreadyExistsException : HttpResponseException
    {
        public TopicAlreadyExistsException(string topic)
        {
            Status = StatusCodes.Status400BadRequest;
            Value = $"Topic '{topic}' already exists.";
        }
    }
}