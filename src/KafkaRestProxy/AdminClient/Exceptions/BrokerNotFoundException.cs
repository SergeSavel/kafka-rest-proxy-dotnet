using Microsoft.AspNetCore.Http;
using pro.savel.KafkaRestProxy.Common.Exceptions;

namespace pro.savel.KafkaRestProxy.AdminClient.Exceptions
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