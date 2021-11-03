namespace SergeSavel.KafkaRestProxy.Common.Exceptions
{
    public class ConfigConflictException : HttpResponseException
    {
        public ConfigConflictException(string parameter) : base($"Conflicting configuration parameter: '{parameter}'.")
        {
            StatusCode = 400;
        }
    }
}