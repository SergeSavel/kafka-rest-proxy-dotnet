using Microsoft.AspNetCore.Http;
using pro.savel.KafkaRestProxy.Common.Exceptions;

namespace pro.savel.KafkaRestProxy.Consumer.Exceptions
{
    public class ValidationException : HttpResponseException
    {
        public ValidationException(string message) : base(message)
        {
            StatusCode = StatusCodes.Status400BadRequest;
            Value = message;
        }
    }
}