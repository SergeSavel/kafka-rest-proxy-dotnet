using System;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using pro.savel.KafkaRestProxy.Entities;

namespace pro.savel.KafkaRestProxy.Services
{
    public class AdminClientService : IDisposable
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
        private readonly IAdminClient _adminClient;
        private readonly ConsumerConfig _consumerConfig;

        public AdminClientService(IConfiguration configuration)
        {
            var adminClientConfig = new AdminClientConfig();
            configuration.Bind("Kafka", adminClientConfig);
            _adminClient = new AdminClientBuilder(adminClientConfig).Build();

            var consumerConfig = new ConsumerConfig();
            configuration.Bind("Kafka", consumerConfig);
            _consumerConfig = consumerConfig;
        }

        public void Dispose()
        {
            _adminClient.Dispose();
        }

        public TopicInfo GetTopicInfo(string topic)
        {
            var metadata = _adminClient.GetMetadata(topic, Timeout);
            if (metadata.Topics.Count == 0) return null;

            var topicMetadata = metadata.Topics[0];

            using var consumer = new ConsumerBuilder<string, string>(_consumerConfig).Build();

            var partitionInfos = topicMetadata.Partitions
                .Select(partitonMetadata => new TopicPartition(topic, partitonMetadata.PartitionId))
                .Select(topicPartition => new
                {
                    Partition = topicPartition,
                    Offsets = consumer.GetWatermarkOffsets(topicPartition)
                })
                .Select(partitionWithOffsets => new TopicInfo.PartitionInfo
                {
                    Name = partitionWithOffsets.Partition.Partition.Value,
                    BeginningOffset = partitionWithOffsets.Offsets.Low,
                    EndOffset = partitionWithOffsets.Offsets.High
                })
                .ToList();

            return new TopicInfo
            {
                Name = topicMetadata.Topic,
                Partitions = partitionInfos
            };
        }
    }
}