﻿using Microsoft.AspNetCore.Http;
using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.Admin.Exceptions
{
    public class BrokerNotFoundException : HttpResponseException
    {
        public BrokerNotFoundException(int brokerId)
        {
            StatusCode = StatusCodes.Status404NotFound;
            Value = $"Broker '{brokerId}' not fpund.";
        }
    }
}