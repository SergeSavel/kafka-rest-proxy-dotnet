using System;
using Confluent.Kafka;
using Metadata = pro.savel.KafkaRestProxy.Entities.Metadata;
using TopicMetadata = pro.savel.KafkaRestProxy.Entities.TopicMetadata;

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
            var metadata = _adminClient.GetMetadata(Timeout);
            return new Metadata(metadata);
        }


        public TopicMetadata GetTopicMetadata(string topic)
        {
            var metadata = _adminClient.GetMetadata(topic, Timeout);

            if (metadata.Topics.Count == 0) return null;

            var topicMetadata = metadata.Topics[0];

            return new TopicMetadata(topicMetadata);

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