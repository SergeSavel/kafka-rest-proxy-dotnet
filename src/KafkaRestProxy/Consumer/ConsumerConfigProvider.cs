using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using pro.savel.KafkaRestProxy.Common;

namespace pro.savel.KafkaRestProxy.Consumer
{
    public static class ConsumerConfigProvider
    {
        public static ConsumerConfig GetConfig(IConfiguration configuration = null)
        {
            var clientConfig = ClientConfigProvider.GetConfig(configuration);

            var result = new ConsumerConfig(clientConfig)
            {
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = "kafka-rest-proxy-dotnet"
            };

            configuration?.Bind("Kafka:Consumer", result);

            return result;
        }
    }
}