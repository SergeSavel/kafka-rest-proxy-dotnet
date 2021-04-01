using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using SergeSavel.KafkaRestProxy.Admin.Contract;
using SergeSavel.KafkaRestProxy.Admin.Exceptions;
using SergeSavel.KafkaRestProxy.Admin.Requests;
using BrokerMetadata = SergeSavel.KafkaRestProxy.Admin.Contract.BrokerMetadata;
using Metadata = SergeSavel.KafkaRestProxy.Admin.Contract.Metadata;
using TopicMetadata = SergeSavel.KafkaRestProxy.Admin.Contract.TopicMetadata;

namespace SergeSavel.KafkaRestProxy.Admin
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
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public Metadata GetMetadata(bool verbose)
        {
            var kafkaMetadata = GetKafkaMetadata();
            return AdminClientMapper.Map(kafkaMetadata, verbose);
        }

        public TopicsMetadata GetTopicsMetadata(bool verbose)
        {
            var kafkaMetadata = GetKafkaMetadata();
            return AdminClientMapper.MapTopics(kafkaMetadata, verbose);
        }

        public TopicMetadata GetTopicMetadata(string topic, bool verbose)
        {
            var kafkaMetadata = GetKafkaMetadata(topic);

            var topicMetadata = kafkaMetadata.Topics[0];

            if (topicMetadata.Error.Code == ErrorCode.UnknownTopicOrPart)
                throw new TopicNotFoundException(topic);

            return AdminClientMapper.Map(topicMetadata, kafkaMetadata, verbose);
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

        private Confluent.Kafka.Metadata GetKafkaMetadata(string topic = null)
        {
            Confluent.Kafka.Metadata result;
            try
            {
                result = topic == null ? _adminClient.GetMetadata(Timeout) : _adminClient.GetMetadata(topic, Timeout);
            }
            catch (KafkaException e)
            {
                throw new AdminClientException("Unable to get metadata.", e);
            }

            return result;
        }

        public async Task<TopicMetadata> CreateTopic(CreateTopicRequest request)
        {
            var topicSpecification = new TopicSpecification
            {
                Name = request.Name,
                NumPartitions = request.NumPartitions ?? -1,
                ReplicationFactor = request.ReplicationFactor ?? -1,
                Configs = request.Config
            };

            var options = new CreateTopicsOptions
            {
                RequestTimeout = Timeout
            };

            try
            {
                await _adminClient.CreateTopicsAsync(new[] {topicSpecification}, options);
            }
            catch (CreateTopicsException e)
            {
                throw new AdminClientException("Unable to create topic.", e);
            }

            return GetTopicMetadata(topicSpecification.Name, true);
        }

        public async Task<ResourceConfig> GetTopicConfigAsync(string topic)
        {
            var resource = new ConfigResource
            {
                Name = topic,
                Type = ResourceType.Topic
            };

            var options = new DescribeConfigsOptions
            {
                RequestTimeout = Timeout
            };

            ICollection<DescribeConfigsResult> result;
            try
            {
                result = await _adminClient.DescribeConfigsAsync(new[] {resource}, options);
            }
            catch (KafkaException e)
            {
                throw new AdminClientException("Unable to get topic config.", e);
            }

            return AdminClientMapper.Map(result.First());
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing) _adminClient.Dispose();
        }
    }
}