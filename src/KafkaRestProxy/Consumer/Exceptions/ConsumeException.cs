using System;
using pro.savel.KafkaRestProxy.Common;

namespace pro.savel.KafkaRestProxy.Consumer.Exceptions
{
    public class ConsumeException : HttpResponseException
    {
        public ConsumeException(Exception innerException) : base(innerException)
        {
        }
    }
}