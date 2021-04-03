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

        public Metadata GetMetadata(bool verbose, int timeoutMs)
        {
            var kafkaMetadata = GetKafkaMetadata(timeoutMs);
            return AdminClientMapper.Map(kafkaMetadata, verbose);
        }

        public TopicsMetadata GetTopicsMetadata(bool verbose, int timeoutMs)
        {
            var kafkaMetadata = GetKafkaMetadata(timeoutMs);
            return AdminClientMapper.MapTopics(kafkaMetadata, verbose);
        }

        public TopicMetadata GetTopicMetadata(string topic, bool verbose, int timeoutMs)
        {
            var kafkaMetadata = GetKafkaMetadata(topic, timeoutMs);

            var topicMetadata = kafkaMetadata.Topics[0];

            if (topicMetadata.Error.Code == ErrorCode.UnknownTopicOrPart)
                throw new TopicNotFoundException(topic);

            return AdminClientMapper.Map(topicMetadata, kafkaMetadata, verbose);
        }

        public async Task CreateTopic(CreateTopicRequest request, int timeoutMs)
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
                RequestTimeout = TimeSpan.FromMilliseconds(timeoutMs)
            };

            try
            {
                await _adminClient.CreateTopicsAsync(new[] {topicSpecification}, options);
            }
            catch (CreateTopicsException e)
            {
                throw new AdminClientException("Unable to create topic.", e);
            }
        }

        public BrokersMetadata GetBrokersMetadata(int timeoutMs)
        {
            var kafkaMetadata = GetKafkaMetadata(timeoutMs);
            return AdminClientMapper.MapBrokers(kafkaMetadata);
        }

        public BrokerMetadata GetBrokerMetadata(int brokerId, int timeoutMs)
        {
            var kafkaMetadata = GetKafkaMetadata(timeoutMs);

            var result = kafkaMetadata.Brokers
                .Where(brokerMetadata => brokerMetadata.BrokerId == brokerId)
                .Select(brokerMetadata => AdminClientMapper.Map(brokerMetadata, kafkaMetadata))
                .FirstOrDefault();

            if (result == null)
                throw new BrokerNotFoundException(brokerId);

            return result;
        }

        private Confluent.Kafka.Metadata GetKafkaMetadata(int timeoutMs)
        {
            var timeout = TimeSpan.FromMilliseconds(timeoutMs);

            Confluent.Kafka.Metadata result;
            try
            {
                result = _adminClient.GetMetadata(timeout);
            }
            catch (KafkaException e)
            {
                throw new AdminClientException("Unable to get metadata.", e);
            }

            return result;
        }

        private Confluent.Kafka.Metadata GetKafkaMetadata(string topic, int timeoutMs)
        {
            var timeout = TimeSpan.FromMilliseconds(timeoutMs);

            Confluent.Kafka.Metadata result;
            try
            {
                result = _adminClient.GetMetadata(topic, timeout);
            }
            catch (KafkaException e)
            {
                throw new AdminClientException("Unable to get metadata.", e);
            }

            return result;
        }

        public async Task<ResourceConfig> GetTopicConfigAsync(string topic, int timeoutMs)
        {
            var resource = new ConfigResource
            {
                Name = topic,
                Type = ResourceType.Topic
            };

            var options = new DescribeConfigsOptions
            {
                RequestTimeout = TimeSpan.FromMilliseconds(timeoutMs)
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

        public async Task<ResourceConfig> GetBrokerConfigAsync(int brokerId, int timeoutMs)
        {
            var resource = new ConfigResource
            {
                Name = brokerId.ToString(),
                Type = ResourceType.Broker
            };

            var options = new DescribeConfigsOptions
            {
                RequestTimeout = TimeSpan.FromMilliseconds(timeoutMs)
            };

            ICollection<DescribeConfigsResult> result;
            try
            {
                result = await _adminClient.DescribeConfigsAsync(new[] {resource}, options);
            }
            catch (KafkaException e)
            {
                throw new AdminClientException("Unable to get broker config.", e);
            }

            return AdminClientMapper.Map(result.First());
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing) _adminClient.Dispose();
        }
    }
}