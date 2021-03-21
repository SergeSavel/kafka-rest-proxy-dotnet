using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using SergeSavel.KafkaRestProxy.Common;

namespace SergeSavel.KafkaRestProxy.Admin
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