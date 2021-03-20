using System;

namespace pro.savel.KafkaRestProxy.Common.Exceptions
{
    public class HttpResponseException : Exception
    {
        public HttpResponseException(string message, Exception innerException) : base(message, innerException)
        {
            Value = message;
        }

        public HttpResponseException(string message) : base(message)
        {
            Value = message;
        }

        public HttpResponseException(Exception innerException) : base(null, innerException)
        {
        }

        public HttpResponseException()
        {
        }

        public int StatusCode { get; init; } = 500;

        public object Value { get; init; }
    }
}