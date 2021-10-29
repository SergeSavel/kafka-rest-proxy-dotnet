using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.AdminClient.Exceptions
{
    public class InvalidParametersException : HttpResponseException
    {
        public InvalidParametersException(string message) : base(message)
        {
            StatusCode = 400;
        }
    }
}