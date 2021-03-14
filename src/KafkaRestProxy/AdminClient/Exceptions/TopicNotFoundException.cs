using Microsoft.AspNetCore.Http;
using pro.savel.KafkaRestProxy.Common;

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