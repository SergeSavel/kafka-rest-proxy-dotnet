// Copyright 2021 Sergey Savelev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using SergeSavel.KafkaRestProxy.AdminClient.Requests;
using SergeSavel.KafkaRestProxy.AdminClient.Responses;

namespace SergeSavel.KafkaRestProxy.AdminClient
{
    public class AdminClientService
    {
        private readonly ILogger<AdminClientService> _logger;
        private readonly AdminClientProvider _provider;

        public AdminClientService(ILogger<AdminClientService> logger, AdminClientProvider provider)
        {
            _logger = logger;
            _provider = provider;
        }

        public ICollection<Responses.AdminClient> ListClients()
        {
            var wrappers = _provider.ListClients();
            return wrappers
                .Select(MapAdminClient)
                .ToList();
        }

        public Responses.AdminClient GetClient(Guid clientId)
        {
            var wrapper = _provider.GetClient(clientId);
            return MapAdminClient(wrapper);
        }

        public AdminClientWithToken CreateClient(CreateAdminClientRequest request, string owner)
        {
            var wrapper = _provider.CreateClient(request.Name, request.Config,
                TimeSpan.FromMilliseconds(request.RetentionMs), owner);
            return MapAdminClientWithToken(wrapper);
        }

        public void RemoveClient(Guid clientId, Guid token)
        {
            var wrapper = _provider.GetClient(clientId, token);
            _provider.RemoveClient(wrapper.Id);
        }

        public Metadata GetMetadata(Guid clientId, Guid token, int timeoutMs)
        {
            var wrapper = _provider.GetClient(clientId, token);
            wrapper.UpdateExpiration();
            return wrapper.GetMetadata(TimeSpan.FromMilliseconds(timeoutMs));
        }

        public BrokersMetadata GetBrokersMetadata(Guid clientId, Guid token, int timeoutMs)
        {
            var wrapper = _provider.GetClient(clientId, token);
            wrapper.UpdateExpiration();
            return wrapper.GetBrokersMetadata(TimeSpan.FromMilliseconds(timeoutMs));
        }

        public BrokerMetadata GetBrokerMetadata(Guid clientId, Guid token, int brokerId, int timeoutMs)
        {
            var wrapper = _provider.GetClient(clientId, token);
            wrapper.UpdateExpiration();
            return wrapper.GetBrokerMetadata(brokerId, TimeSpan.FromMilliseconds(timeoutMs));
        }

        public TopicsMetadata GetTopicsMetadata(Guid clientId, Guid token, int timeoutMs)
        {
            var wrapper = _provider.GetClient(clientId, token);
            wrapper.UpdateExpiration();
            return wrapper.GetTopicsMetadata(TimeSpan.FromMilliseconds(timeoutMs));
        }

        public TopicMetadata GetTopicMetadata(Guid clientId, Guid token, string topic, int timeoutMs)
        {
            var wrapper = _provider.GetClient(clientId, token);
            wrapper.UpdateExpiration();
            return wrapper.GetTopicMetadata(topic, TimeSpan.FromMilliseconds(timeoutMs));
        }

        public async Task CreateTopicAsync(Guid clientId, Guid token, CreateTopicRequest request, int timeoutMs)
        {
            var wrapper = _provider.GetClient(clientId, token);
            wrapper.UpdateExpiration();
            await wrapper.CreateTopicAsync(request.Topic, request.NumPartitions, request.ReplicationFactor,
                request.Config, TimeSpan.FromMilliseconds(timeoutMs));
        }

        public async Task<ResourceConfig> GetTopicConfigAsync(Guid clientId, Guid token, string topic, int timeoutMs)
        {
            var wrapper = _provider.GetClient(clientId, token);
            wrapper.UpdateExpiration();
            return await wrapper.GetTopicConfigAsync(topic, TimeSpan.FromMilliseconds(timeoutMs));
        }

        public async Task<ResourceConfig> GetBrokerConfigAsync(Guid clientId, Guid token, int brokerId, int timeoutMs)
        {
            var wrapper = _provider.GetClient(clientId, token);
            wrapper.UpdateExpiration();
            return await wrapper.GetBrokerConfigAsync(brokerId, TimeSpan.FromMilliseconds(timeoutMs));
        }

        private static Responses.AdminClient MapAdminClient(AdminClientWrapper wrapper)
        {
            return new Responses.AdminClient
            {
                Id = wrapper.Id,
                ExpiresAt = wrapper.ExpiresAt,
                Owner = wrapper.Owner
            };
        }

        private static AdminClientWithToken MapAdminClientWithToken(AdminClientWrapper wrapper)
        {
            return new AdminClientWithToken
            {
                Id = wrapper.Id,
                ExpiresAt = wrapper.ExpiresAt,
                Owner = wrapper.Owner,
                Token = wrapper.Token
            };
        }
    }
}