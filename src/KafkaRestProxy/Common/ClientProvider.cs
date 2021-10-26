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
using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.Common
{
    public abstract class ClientProvider<TClientWrapper> : IDisposable where TClientWrapper : ClientWrapper
    {
        private readonly ConcurrentDictionary<Guid, TClientWrapper> _wrappers = new();

        public void Dispose()
        {
            var items = ListItems();
            foreach (var item in items) item.Dispose();
        }

        protected IList<TClientWrapper> ListItems()
        {
            return _wrappers.Values.ToList();
        }

        protected void AddItem(TClientWrapper wrapper)
        {
            if (!_wrappers.TryAdd(wrapper.Id, wrapper))
            {
                wrapper.Dispose();
                throw new Exception("Unable to register consumer.");
            }
        }

        protected TClientWrapper GetItem(Guid id)
        {
            if (!_wrappers.TryGetValue(id, out var consumerWrapper))
                throw new ClientNotFoundException(id);
            return consumerWrapper;
        }

        protected void TryGetItem(Guid id, out TClientWrapper wrapper)
        {
            _wrappers.TryGetValue(id, out wrapper);
        }

        protected bool RemoveItem(Guid id)
        {
            if (!_wrappers.TryRemove(id, out var wrapper)) return false;
            Task.Run(() => wrapper.Dispose());
            return true;
        }

        public void RemoveExpiredItems()
        {
            foreach (var wrapper in _wrappers.Values)
                if (wrapper.IsExpired)
                    RemoveItem(wrapper.Id);
        }
    }
}