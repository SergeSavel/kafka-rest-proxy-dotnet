using System;
using Microsoft.AspNetCore.Http;
using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.Consumer.Exceptions
{
    public class ConsumerNotFoundException : HttpResponseException
    {
        public ConsumerNotFoundException(Guid consumerId)
        {
            StatusCode = StatusCodes.Status404NotFound;
        }
    }
}