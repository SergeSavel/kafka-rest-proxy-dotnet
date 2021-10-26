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
using SergeSavel.KafkaRestProxy.Common;
using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.AdminClient
{
    public class AdminClientProvider : ClientProvider<AdminClientWrapper>
    {
        private readonly AdminClientConfig _defaultConfig;

        public AdminClientProvider(AdminClientConfig defaultConfig)
        {
            _defaultConfig = defaultConfig;
        }

        public AdminClientWrapper CreateClient(string name, IEnumerable<KeyValuePair<string, string>> config,
            TimeSpan retention, string owner = null)
        {
            var effectiveConfig = new Dictionary<string, string>();
            foreach (var (key, value) in _defaultConfig)
                effectiveConfig.Add(key, value);
            foreach (var (key, value) in config)
                effectiveConfig.Add(key, value);

            var wrapper = new AdminClientWrapper(name, effectiveConfig, retention)
            {
                Owner = owner
            };

            AddItem(wrapper);

            return wrapper;
        }

        public ICollection<AdminClientWrapper> ListClients()
        {
            return ListItems();
        }

        public AdminClientWrapper GetClient(Guid id, Guid? token = null)
        {
            var wrapper = GetItem(id);
            if (token != null && wrapper.Token != token)
                throw new InvalidTokenException(id);
            return wrapper;
        }

        public void RemoveClient(Guid id, Guid? token = null)
        {
            var wrapper = GetItem(id);
            if (token != null && wrapper.Token != token)
                throw new InvalidTokenException(id);
            RemoveItem(id);
        }
    }
}