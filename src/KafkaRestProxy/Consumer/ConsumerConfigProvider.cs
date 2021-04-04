using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using SergeSavel.KafkaRestProxy.Common;

namespace SergeSavel.KafkaRestProxy.Consumer
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

            //result.Debug = "all";

            configuration?.Bind("Kafka:Consumer", result);

            return result;
        }
    }
}