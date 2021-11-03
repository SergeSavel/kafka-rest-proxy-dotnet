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
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.SchemaRegistry;

namespace SergeSavel.KafkaRestProxy.Producer
{
    public class ProducerProvider : ClientProvider<ProducerWrapper>
    {
        private readonly ProducerConfig _defaultConfig;
        private readonly ILogger<ProducerProvider> _logger;
        private readonly SchemaRegistryService _schemaRegistryService;

        public ProducerProvider(ILogger<ProducerProvider> logger, ProducerConfig defaultConfig,
            SchemaRegistryService schemaRegistryService)
        {
            _logger = logger;
            _defaultConfig = defaultConfig;
            _schemaRegistryService = schemaRegistryService;
        }

        public ProducerWrapper CreateProducer(string name, IEnumerable<KeyValuePair<string, string>> config,
            TimeSpan expirationTimeout, string owner = null)
        {
            var effectiveConfig = new Dictionary<string, string>();
            foreach (var (key, value) in _defaultConfig)
                effectiveConfig[key] = value;
            foreach (var (key, value) in config)
                effectiveConfig[key] = value;

            var wrapper = new ProducerWrapper(name, effectiveConfig, _schemaRegistryService.Client, expirationTimeout)
            {
                Owner = owner
            };

            AddItem(wrapper);

            return wrapper;
        }
    }
}