using System;

namespace pro.savel.KafkaRestProxy.Common
{
    public class HttpResponseException : Exception
    {
        public int StatusCode { get; init; } = 500;

        public object Value { get; init; }
    }
}