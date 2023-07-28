// Copyright 2023 Sergey Savelev
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Confluent.Kafka;
using Microsoft.Extensions.Options;
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.Common.Contract;
using SergeSavel.KafkaRestProxy.SchemaRegistry;

namespace SergeSavel.KafkaRestProxy.Consumer;

public class ConsumerProvider : ClientProvider<ConsumerWrapper>
{
    private readonly IDictionary<string, string> _defaultConfig;
    private readonly ILogger<ConsumerProvider> _logger;
    private readonly SchemaRegistryService _schemaRegistryService;

    public ConsumerProvider(ILogger<ConsumerProvider> logger, IOptions<ClientConfig> clientConfigOptions,
        IOptions<ConsumerConfig> consumerConfigOptions, SchemaRegistryService schemaRegistryService)
    {
        _logger = logger;
        _defaultConfig = EffectiveConfig(clientConfigOptions.Value, consumerConfigOptions.Value);
        _schemaRegistryService = schemaRegistryService;
    }

    public ConsumerWrapper CreateConsumer(string name, IEnumerable<KeyValuePair<string, string>> config,
        KeyValueType keyType, KeyValueType valueType, TimeSpan expirationTimeout, string owner = null)
    {
        _logger.LogDebug("Creating consumer '{Name}'", name);

        var effectiveConfig = EffectiveConfig(_defaultConfig, config);

        var wrapper = new ConsumerWrapper(name, effectiveConfig, keyType, valueType, _schemaRegistryService.Client,
            expirationTimeout)
        {
            Owner = owner
        };

        AddItem(wrapper);

        return wrapper;
    }

    private static IDictionary<string, string> EffectiveConfig(IEnumerable<KeyValuePair<string, string>> config1,
        IEnumerable<KeyValuePair<string, string>> config2)
    {
        var effectiveConfig = new Dictionary<string, string>();
        foreach (var (key, value) in config1)
            effectiveConfig[key] = value;
        foreach (var (key, value) in config2)
            effectiveConfig[key] = value;
        return effectiveConfig;
    }
}