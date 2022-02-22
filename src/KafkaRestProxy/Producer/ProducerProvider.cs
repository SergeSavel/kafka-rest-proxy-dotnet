﻿// Copyright 2021 Sergey Savelev
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

using Confluent.Kafka;
using Microsoft.Extensions.Options;
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.SchemaRegistry;

namespace SergeSavel.KafkaRestProxy.Producer;

public class ProducerProvider : ClientProvider<ProducerWrapper>
{
    private readonly IDictionary<string, string> _defaultConfig;
    private readonly ILogger<ProducerProvider> _logger;
    private readonly SchemaRegistryService _schemaRegistryService;

    public ProducerProvider(ILogger<ProducerProvider> logger, IOptions<ClientConfig> clientConfigOptions,
        IOptions<ProducerConfig> producerConfigOptions,
        SchemaRegistryService schemaRegistryService)
    {
        _logger = logger;
        _defaultConfig = EffectiveConfig(clientConfigOptions.Value, producerConfigOptions.Value);
        _schemaRegistryService = schemaRegistryService;
    }

    public ProducerWrapper CreateProducer(string name, IEnumerable<KeyValuePair<string, string>> config,
        TimeSpan expirationTimeout, string owner = null)
    {
        var effectiveConfig = EffectiveConfig(_defaultConfig, config);

        var wrapper = new ProducerWrapper(name, effectiveConfig, _schemaRegistryService.Client, expirationTimeout)
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