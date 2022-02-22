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
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.AdminClient
{
    public class AdminClientProvider : ClientProvider<AdminClientWrapper>
    {
        private readonly ILogger<AdminClientProvider> _logger;
        private readonly IDictionary<string, string> _defaultConfig;
        
        public AdminClientProvider(ILogger<AdminClientProvider> logger, IOptions<ClientConfig> clientConfigOptions,
            IOptions<AdminClientConfig> adminClientConfigOptions)
        {
            _logger = logger;
            _defaultConfig = EffectiveConfig(clientConfigOptions.Value, adminClientConfigOptions.Value);
        }

        public AdminClientWrapper CreateClient(string name, IDictionary<string, string> config,
            TimeSpan expirationTimeout, string owner = null)
        {
            var effectiveConfig = EffectiveConfig(_defaultConfig, config);
            var wrapper = new AdminClientWrapper(name, effectiveConfig, expirationTimeout)
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
}