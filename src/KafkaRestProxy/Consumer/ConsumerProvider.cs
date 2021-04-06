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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SergeSavel.KafkaRestProxy.Consumer.Exceptions;

namespace SergeSavel.KafkaRestProxy.Consumer
{
    public class ConsumerProvider : IDisposable
    {
        private readonly ConcurrentDictionary<Guid, ConsumerWrapper> _consumers = new();

        public void Dispose()
        {
            var wrappers = ListConsumers();
            foreach (var wrapper in wrappers)
                try
                {
                    wrapper.Dispose();
                }
                catch (ObjectDisposedException)
                {
                }
        }

        public ConsumerWrapper CreateConsumer(IDictionary<string, string> consumerConfig, TimeSpan expirationTimeout,
            string creator = null)
        {
            var consumerWrapper = new ConsumerWrapper(consumerConfig, expirationTimeout)
            {
                Creator = creator
            };

            if (!_consumers.TryAdd(consumerWrapper.Id, consumerWrapper))
            {
                consumerWrapper.Dispose();
                throw new Exception("Unable to register consumer.");
            }

            return consumerWrapper;
        }

        public ConsumerWrapper GetConsumer(Guid id)
        {
            if (!_consumers.TryGetValue(id, out var consumerWrapper))
                throw new ConsumerNotFoundException(id);
            return consumerWrapper;
        }

        public void TryGetConsumer(Guid id, out ConsumerWrapper consumerWrapper)
        {
            _consumers.TryGetValue(id, out consumerWrapper);
        }

        public ICollection<ConsumerWrapper> ListConsumers()
        {
            return _consumers.Values.ToList();
        }

        public bool RemoveConsumer(Guid id)
        {
            if (!_consumers.TryRemove(id, out var consumerWrapper)) return false;

            Task.Run(() =>
            {
                try
                {
                    consumerWrapper.Consumer.Close();
                }
                finally
                {
                    consumerWrapper.Dispose();
                }
            });

            return true;
        }

        public void RemoveExpiredConsumers()
        {
            foreach (var consumerWrapper in _consumers.Values)
                if (consumerWrapper.IsExpired)
                    RemoveConsumer(consumerWrapper.Id);
        }
    }
}