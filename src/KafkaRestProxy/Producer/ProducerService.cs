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

using SergeSavel.KafkaRestProxy.Producer.Requests;
using SergeSavel.KafkaRestProxy.Producer.Responses;

namespace SergeSavel.KafkaRestProxy.Producer;

public class ProducerService
{
    private readonly ILogger<ProducerService> _logger;
    private readonly ProducerProvider _provider;

    public ProducerService(ILogger<ProducerService> logger, ProducerProvider provider)
    {
        _logger = logger;
        _provider = provider;
    }

    public ICollection<Responses.Producer> ListProducers()
    {
        var wrappers = _provider.ListItems();
        return wrappers
            .Select(MapProducer)
            .ToList();
    }

    public Responses.Producer GetProducer(Guid producerId)
    {
        var wrapper = _provider.GetItem(producerId);
        return MapProducer(wrapper);
    }

    public ProducerWithToken CreateProducer(CreateProducerRequest request, string owner)
    {
        var wrapper = _provider.CreateProducer(request.Name, request.Config, request.KeyType, request.ValueType,
            TimeSpan.FromMilliseconds(request.ExpirationTimeoutMs), owner);
        return MapProducerWithToken(wrapper);
    }

    public void RemoveProducer(Guid producerId, string token)
    {
        var wrapper = _provider.GetItem(producerId, token);
        _provider.RemoveItem(wrapper.Id);
    }

    public async Task<DeliveryResult> ProduceAsync(Guid producerId, string token, string topic, int? partition,
        PostMessageRequest request)
    {
        var wrapper = _provider.GetItem(producerId, token);
        wrapper.UpdateExpiration();
        return await wrapper.ProduceAsync(topic, partition, request).ConfigureAwait(false);
    }

    private static Responses.Producer MapProducer(ProducerWrapper source)
    {
        return new Responses.Producer
        {
            Id = source.Id,
            Name = source.Name,
            User = source.User,
            ExpiresAt = source.ExpiresAt,
            Owner = source.Owner
        };
    }

    private static ProducerWithToken MapProducerWithToken(ProducerWrapper source)
    {
        return new ProducerWithToken
        {
            Id = source.Id,
            Name = source.Name,
            User = source.User,
            ExpiresAt = source.ExpiresAt,
            Owner = source.Owner,
            Token = source.Token
        };
    }
}