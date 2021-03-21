using Microsoft.AspNetCore.Http;
using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.Consumer.Exceptions
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