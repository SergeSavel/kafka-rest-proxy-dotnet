using Microsoft.AspNetCore.Http;
using pro.savel.KafkaRestProxy.Common;

namespace pro.savel.KafkaRestProxy.Consumer.Exceptions
{
    public class ValidationException : HttpResponseException
    {
        public ValidationException(string message)
        {
            StatusCode = StatusCodes.Status400BadRequest;
            Value = message;
        }
    }
}