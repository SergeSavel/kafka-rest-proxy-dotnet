using System;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using SergeSavel.KafkaRestProxy.AdminClient.Contract;
using SergeSavel.KafkaRestProxy.AdminClient.Exceptions;
using BrokerMetadata = SergeSavel.KafkaRestProxy.AdminClient.Contract.BrokerMetadata;
using KafkaException = SergeSavel.KafkaRestProxy.Common.Exceptions.KafkaException;
using Metadata = SergeSavel.KafkaRestProxy.AdminClient.Contract.Metadata;
using TopicMetadata = SergeSavel.KafkaRestProxy.AdminClient.Contract.TopicMetadata;

namespace SergeSavel.KafkaRestProxy.AdminClient
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
            var kafkaMetadata = GetKafkaMetadata();
            return AdminClientMapper.Map(kafkaMetadata);
        }

        public TopicsMetadata GetTopicsMetadata()
        {
            var kafkaMetadata = GetKafkaMetadata();
            return AdminClientMapper.MapTopics(kafkaMetadata);
        }

        public TopicMetadata GetTopicMetadata(string topic)
        {
            var kafkaMetadata = GetKafkaMetadata();

            var topicMetadata = kafkaMetadata.Topics[0];

            if (topicMetadata.Error.Code == ErrorCode.UnknownTopicOrPart)
                throw new TopicNotFoundException(topic);

            return AdminClientMapper.Map(topicMetadata, kafkaMetadata);
        }

        public BrokersMetadata GetBrokersMetadata()
        {
            var kafkaMetadata = GetKafkaMetadata();
            return AdminClientMapper.MapBrokers(kafkaMetadata);
        }

        public BrokerMetadata GetBrokerMetadata(int brokerId)
        {
            var kafkaMetadata = GetKafkaMetadata();

            var result = kafkaMetadata.Brokers
                .Where(brokerMetadata => brokerMetadata.BrokerId == brokerId)
                .Select(brokerMetadata => AdminClientMapper.Map(brokerMetadata, kafkaMetadata))
                .FirstOrDefault();

            if (result == null)
                throw new BrokerNotFoundException(brokerId);

            return result;
        }

        public async Task<TopicMetadata> CreateTopic(string topic, int? numPartitions = null,
            short? replicationFactor = null)
        {
            var topicSpecification = new TopicSpecification
            {
                Name = topic,
                NumPartitions = numPartitions ?? -1,
                ReplicationFactor = replicationFactor ?? -1
            };

            try
            {
                await _adminClient.CreateTopicsAsync(new[] {topicSpecification});
            }
            catch (CreateTopicsException e)
            {
                if (e.Results.Any(result => result.Error.Code == ErrorCode.TopicAlreadyExists))
                    throw new TopicAlreadyExistsException(topic);
                throw new KafkaException("Unable to create topic.", e);
            }

            return GetTopicMetadata(topic);
        }

        private Confluent.Kafka.Metadata GetKafkaMetadata()
        {
            Confluent.Kafka.Metadata result;
            try
            {
                result = _adminClient.GetMetadata(Timeout);
            }
            catch (Confluent.Kafka.KafkaException e)
            {
                throw new KafkaException("Unable to get metadata.", e);
            }

            return result;
        }
    }
}