using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using pro.savel.KafkaRestProxy.Common;

namespace pro.savel.KafkaRestProxy.Producer
{
    public static class ProducerConfigProvider
    {
        public static ProducerConfig GetConfig(IConfiguration configuration = null)
        {
            var clientConfig = ClientConfigProvider.GetConfig(configuration);

            var result = new ProducerConfig(clientConfig)
            {
                Acks = Acks.All,
                Partitioner = Partitioner.ConsistentRandom,
                CompressionType = CompressionType.Snappy
            };

            configuration?.Bind("Kafka:Producer", result);

            return result;
        }
    }
}