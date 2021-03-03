using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace pro.savel.KafkaRestProxy.Common
{
    public static class ClientConfigProvider
    {
        public static ClientConfig GetConfig(IConfiguration configuration = null)
        {
            var result = new ClientConfig
            {
                ClientId = "kafka-rest-proxy-dotnet"
            };

            configuration?.Bind("Kafka", result);

            return result;
        }
    }
}