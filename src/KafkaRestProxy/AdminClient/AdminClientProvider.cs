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
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.AdminClient
{
    public class AdminClientProvider : ClientProvider<AdminClientWrapper>
    {
        private readonly AdminClientConfig _defaultConfig;
        private readonly ILogger<AdminClientProvider> _logger;

        public AdminClientProvider(ILogger<AdminClientProvider> logger, AdminClientConfig defaultConfig)
        {
            _logger = logger;
            _defaultConfig = defaultConfig;
        }

        public AdminClientWrapper CreateClient(string name, IDictionary<string, string> config,
            TimeSpan expirationTimeout, string owner = null)
        {
            var effectiveConfig = GetEffectiveConfig(config);
            var wrapper = new AdminClientWrapper(name, effectiveConfig, expirationTimeout)
            {
                Owner = owner
            };
            AddItem(wrapper);
            return wrapper;
        }

        private IDictionary<string, string> GetEffectiveConfig(IDictionary<string, string> config)
        {
            //ValidateStrictParameter(config, "bootstrap.servers");
            var effectiveConfig = new Dictionary<string, string>();
            foreach (var (key, value) in _defaultConfig)
                effectiveConfig[key] = value;
            foreach (var (key, value) in config)
                effectiveConfig[key] = value;
            return effectiveConfig;
        }

        private void ValidateStrictParameter(IDictionary<string, string> config, string parameterName)
        {
            var defaultValue = _defaultConfig.Get(parameterName);
            if (defaultValue != null)
            {
                var value = config
                    .Where(kv => string.Equals(kv.Key, parameterName, StringComparison.OrdinalIgnoreCase))
                    .Select(kv => kv.Value)
                    .FirstOrDefault();
                if (value != null)
                {
                    var defaultList = defaultValue
                        .Split(",", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                    var list = value
                        .Split(",", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                    if (!defaultList.SequenceEqual(list, StringComparer.OrdinalIgnoreCase))
                        throw new ConfigConflictException(parameterName);
                }
            }
        }
    }
}