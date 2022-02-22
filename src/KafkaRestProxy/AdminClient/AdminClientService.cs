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

using SergeSavel.KafkaRestProxy.AdminClient.Requests;
using SergeSavel.KafkaRestProxy.AdminClient.Responses;
using SergeSavel.KafkaRestProxy.Common.Responses;

namespace SergeSavel.KafkaRestProxy.AdminClient;

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
        var wrappers = _provider.ListItems();
        return wrappers
            .Select(MapAdminClient)
            .ToList();
    }

    public Responses.AdminClient GetClient(Guid clientId)
    {
        var wrapper = _provider.GetItem(clientId);
        return MapAdminClient(wrapper);
    }

    public AdminClientWithToken CreateClient(CreateAdminClientRequest request, string owner)
    {
        var wrapper = _provider.CreateClient(request.Name, request.Config,
            TimeSpan.FromMilliseconds(request.ExpirationTimeoutMs), owner);
        return MapAdminClientWithToken(wrapper);
    }

    public void RemoveClient(Guid clientId, string token)
    {
        var wrapper = _provider.GetItem(clientId, token);
        _provider.RemoveItem(wrapper.Id);
    }

    public Metadata GetMetadata(Guid clientId, string token, TimeSpan timeout)
    {
        var wrapper = _provider.GetItem(clientId, token);
        wrapper.UpdateExpiration();
        return wrapper.GetMetadata(timeout);
    }

    public Metadata GetMetadata(Guid clientId, string token, string topic, TimeSpan timeout)
    {
        var wrapper = _provider.GetItem(clientId, token);
        wrapper.UpdateExpiration();
        return wrapper.GetMetadata(topic, timeout);
    }

    public async Task CreateTopicAsync(Guid clientId, string token, CreateTopicRequest request, TimeSpan timeout)
    {
        var wrapper = _provider.GetItem(clientId, token);
        wrapper.UpdateExpiration();
        await wrapper.CreateTopicAsync(request.Topic, request.NumPartitions, request.ReplicationFactor,
            request.Config, timeout);
    }

    public async Task<ResourceConfig> GetTopicConfigAsync(Guid clientId, string token, string topic,
        TimeSpan timeout)
    {
        var wrapper = _provider.GetItem(clientId, token);
        wrapper.UpdateExpiration();
        return await wrapper.GetTopicConfigAsync(topic, timeout);
    }

    public async Task<ResourceConfig> GetBrokerConfigAsync(Guid clientId, string token, int brokerId,
        TimeSpan timeout)
    {
        var wrapper = _provider.GetItem(clientId, token);
        wrapper.UpdateExpiration();
        return await wrapper.GetBrokerConfigAsync(brokerId, timeout);
    }

    private static Responses.AdminClient MapAdminClient(AdminClientWrapper wrapper)
    {
        return new Responses.AdminClient
        {
            Id = wrapper.Id,
            Name = wrapper.Name,
            User = wrapper.User,
            ExpiresAt = wrapper.ExpiresAt,
            Owner = wrapper.Owner
        };
    }

    private static AdminClientWithToken MapAdminClientWithToken(AdminClientWrapper wrapper)
    {
        return new AdminClientWithToken
        {
            Id = wrapper.Id,
            Name = wrapper.Name,
            User = wrapper.User,
            ExpiresAt = wrapper.ExpiresAt,
            Owner = wrapper.Owner,
            Token = wrapper.Token
        };
    }
}