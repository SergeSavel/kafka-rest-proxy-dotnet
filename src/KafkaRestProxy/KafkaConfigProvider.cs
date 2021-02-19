using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace pro.savel.KafkaRestProxy
{
    public static class KafkaConfigProvider
    {
        public static ClientConfig GetClientConfig(IConfiguration configuration = null)
        {
            var result = new ClientConfig
            {
                ClientId = "kafka-proxy-dotnet",
                Acks = Acks.All
            };

            configuration?.Bind("Kafka", result);

            return result;
        }

        public static AdminClientConfig GetAdminClientConfig(IConfiguration configuration = null)
        {
            var clientConfig = GetClientConfig(configuration);

            var result = new AdminClientConfig(clientConfig);

            configuration?.Bind("Kafka:AdminClient", result);

            return result;
        }

        public static ProducerConfig GetProducerConfig(IConfiguration configuration = null)
        {
            var clientConfig = GetClientConfig(configuration);

            var result = new ProducerConfig(clientConfig)
            {
                Partitioner = Partitioner.ConsistentRandom
            };

            configuration?.Bind("Kafka:Producer", result);

            return result;
        }

        public static ConsumerConfig GetConsumerConfig(IConfiguration configuration = null)
        {
            var clientConfig = GetClientConfig(configuration);

            var result = new ConsumerConfig(clientConfig)
            {
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            configuration?.Bind("Kafka:Consumer", result);

            return result;
        }
    }
}