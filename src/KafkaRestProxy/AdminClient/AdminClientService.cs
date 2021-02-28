using System;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using pro.savel.KafkaRestProxy.AdminClient.Responses;
using BrokerMetadata = pro.savel.KafkaRestProxy.AdminClient.Responses.BrokerMetadata;
using Metadata = pro.savel.KafkaRestProxy.AdminClient.Responses.Metadata;
using TopicMetadata = pro.savel.KafkaRestProxy.AdminClient.Responses.TopicMetadata;

namespace pro.savel.KafkaRestProxy.AdminClient
{
    public class AdminClientService : IDisposable
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
        private readonly IAdminClient _adminClient;

        public AdminClientService(AdminClientConfig adminClientConfig)
        {
            _adminClient = new AdminClientBuilder(adminClientConfig).Build();
        }

        public void Dispose()
        {
            _adminClient.Dispose();
        }

        public Metadata GetMetadata()
        {
            var adminClientMetadata = _adminClient.GetMetadata(Timeout);
            var metadata = AdminClientMapper.Map(adminClientMetadata);

            return metadata;
        }

        public TopicsMetadata GetTopicsMetadata()
        {
            var adminClientMetadata = _adminClient.GetMetadata(Timeout);

            return AdminClientMapper.MapTopics(adminClientMetadata);
        }

        public TopicMetadata GetTopicMetadata(string topic)
        {
            var adminClientMetadata = _adminClient.GetMetadata(topic, Timeout);

            return adminClientMetadata.Topics
                .Select(topicMetadata => AdminClientMapper.Map(topicMetadata, adminClientMetadata))
                .FirstOrDefault();
        }

        public async Task CreateTopic(string topic, int? numPartitions = null, short? replicationFactor = null)
        {
            var topicSpecification = new TopicSpecification
            {
                Name = topic,
                NumPartitions = numPartitions ?? -1,
                ReplicationFactor = replicationFactor ?? -1
            };

            await _adminClient.CreateTopicsAsync(new[] {topicSpecification});
        }

        public BrokersMetadata GetBrokersMetadata()
        {
            var adminClientMetadata = _adminClient.GetMetadata(Timeout);

            return AdminClientMapper.MapBrokers(adminClientMetadata);
        }

        public BrokerMetadata GetBrokerMetadata(int brokerId)
        {
            var adminClientMetadata = _adminClient.GetMetadata(Timeout);

            return adminClientMetadata.Brokers
                .Where(brokerMetadata => brokerMetadata.BrokerId == brokerId)
                .Select(brokerMetadata => AdminClientMapper.Map(brokerMetadata, adminClientMetadata))
                .FirstOrDefault();
        }
    }
}