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

using System.Collections.Concurrent;
using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.Common;

public abstract class ClientProvider<TClientWrapper> : IDisposable where TClientWrapper : ClientWrapper
{
    private readonly ConcurrentDictionary<Guid, TClientWrapper> _wrappers = new();

    public void Dispose()
    {
        var items = ListItems();
        foreach (var item in items) item.Dispose();
    }

    public IList<TClientWrapper> ListItems()
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

    public TClientWrapper GetItem(Guid id, string token = null)
    {
        if (!_wrappers.TryGetValue(id, out var matchedWrapper)) throw new ClientNotFoundException(id);
        if (token != null && !token.Equals(matchedWrapper.Token)) throw new InvalidTokenException(id);
        return matchedWrapper;
    }

    public bool TryGetItem(Guid id, out TClientWrapper wrapper, string token = null)
    {
        if (!_wrappers.TryGetValue(id, out var matchedWrapper))
        {
            wrapper = null;
            return false;
        }

        if (token != null && !token.Equals(matchedWrapper.Token)) throw new InvalidTokenException(id);
        wrapper = matchedWrapper;
        return true;
    }

    public void RemoveItem(Guid id, string token = null)
    {
        if (!_wrappers.TryGetValue(id, out var matchedWrapper)) return;
        if (token != null && !token.Equals(matchedWrapper.Token)) throw new InvalidTokenException(id);
        if (_wrappers.TryRemove(id, out matchedWrapper))
            Task.Run(() => matchedWrapper.Dispose());
    }
}