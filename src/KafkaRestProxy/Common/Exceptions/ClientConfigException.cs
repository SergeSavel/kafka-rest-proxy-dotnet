namespace SergeSavel.KafkaRestProxy.Common.Exceptions;

public class ClientConfigException : HttpResponseException
{
    public ClientConfigException(string message) : base(message)
    {
        StatusCode = 400;
    }
    
    public ClientConfigException(string message, Exception innerException) : base(message, innerException)
    {
        StatusCode = 400;
    }
    
    public ClientConfigException(Exception innerException) : base("Invalid client configuration.", innerException)
    {
        StatusCode = 400;
    }
}