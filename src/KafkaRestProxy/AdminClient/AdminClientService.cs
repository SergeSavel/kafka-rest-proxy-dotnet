﻿using System;
using Confluent.Kafka;
using Metadata = pro.savel.KafkaRestProxy.AdminClient.Contract.Metadata;
using TopicMetadata = pro.savel.KafkaRestProxy.AdminClient.Contract.TopicMetadata;

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

        public TopicMetadata GetTopicMetadata(string topic)
        {
            var adminClientMetadata = _adminClient.GetMetadata(topic, Timeout);

            if (adminClientMetadata.Topics.Count == 0) return null;

            var adminClientTopicMetadata = adminClientMetadata.Topics[0];
            var topicMetadata = AdminClientMapper.Map(adminClientTopicMetadata);

            return topicMetadata;
        }
    }
}