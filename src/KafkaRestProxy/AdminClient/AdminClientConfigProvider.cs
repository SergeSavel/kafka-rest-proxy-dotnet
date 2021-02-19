using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using pro.savel.KafkaRestProxy.Common;

namespace pro.savel.KafkaRestProxy.AdminClient
{
    public static class AdminClientConfigProvider
    {
        public static AdminClientConfig GetConfig(IConfiguration configuration = null)
        {
            var clientConfig = ClientConfigProvider.GetConfig(configuration);

            var result = new AdminClientConfig(clientConfig);

            configuration?.Bind("Kafka:AdminClient", result);

            return result;
        }
    }
}