using Microsoft.AspNetCore.Http;
using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.AdminClient.Exceptions
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