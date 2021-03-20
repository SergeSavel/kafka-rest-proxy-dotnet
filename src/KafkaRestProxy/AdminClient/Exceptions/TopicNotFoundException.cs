using Microsoft.AspNetCore.Http;
using pro.savel.KafkaRestProxy.Common.Exceptions;

namespace pro.savel.KafkaRestProxy.AdminClient.Exceptions
{
    public class TopicNotFoundException : HttpResponseException
    {
        public TopicNotFoundException(string topic)
        {
            StatusCode = StatusCodes.Status404NotFound;
            Value = $"Topic '{topic}' not found.";
        }
    }
}