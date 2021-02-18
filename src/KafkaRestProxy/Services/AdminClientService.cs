using System;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using pro.savel.KafkaRestProxy.Entities;

namespace pro.savel.KafkaRestProxy.Services
{
    public class AdminClientService : IDisposable
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
        private readonly IAdminClient _adminClient;
        private readonly ConsumerConfig _consumerConfig;

        public AdminClientService(AdminClientConfig adminClientConfig, ConsumerConfig consumerConfig)
        {
            _adminClient = new AdminClientBuilder(adminClientConfig).Build();
            _consumerConfig = consumerConfig;
        }

        public void Dispose()
        {
            _adminClient.Dispose();
        }

        public Metadata GetMetadata()
        {
            return _adminClient.GetMetadata(Timeout);
        }
        
        
        
        public TopicMetadata GetTopicMetadata(string topic)
        {
            var metadata = _adminClient.GetMetadata(topic, Timeout);
            if (metadata.Topics.Count == 0) return null;

            return metadata.Topics[0];

            // using var consumer = new ConsumerBuilder<string, string>(_consumerConfig).Build();
            //
            // var partitionInfos = topicMetadata.Partitions
            //     .Select(partitionMetadata => new TopicPartition(topic, partitionMetadata.PartitionId))
            //     .Select(topicPartition => new
            //     {
            //         Partition = topicPartition,
            //         Offsets = consumer.GetWatermarkOffsets(topicPartition)
            //     })
            //     .Select(partitionWithOffsets => new TopicInfo.PartitionInfo
            //     {
            //         Id = partitionWithOffsets.Partition.Partition.Value,
            //         BeginningOffset = partitionWithOffsets.Offsets.Low,
            //         EndOffset = partitionWithOffsets.Offsets.High
            //     })
            //     .ToList();
            //
            // return new TopicInfo
            // {
            //     Name = topicMetadata.Topic,
            //     Partitions = partitionInfos
            // };
        }
    }
}