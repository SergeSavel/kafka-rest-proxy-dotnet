using System;
using Microsoft.AspNetCore.Http;
using pro.savel.KafkaRestProxy.Common;

namespace pro.savel.KafkaRestProxy.Consumer.Exceptions
{
    public class ConsumerNotFoundException : HttpResponseException
    {
        public ConsumerNotFoundException(Guid consumerId)
        {
            StatusCode = StatusCodes.Status404NotFound;
        }
    }
}