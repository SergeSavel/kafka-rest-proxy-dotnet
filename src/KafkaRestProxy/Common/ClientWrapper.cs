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

namespace SergeSavel.KafkaRestProxy.Common
{
    public abstract class ClientWrapper : IDisposable
    {
        private readonly TimeSpan _expirationTimeout;

        protected ClientWrapper(string name, TimeSpan expirationTimeout)
        {
            Name = name;
            _expirationTimeout = expirationTimeout;
            UpdateExpiration();
        }

        public Guid Id { get; } = Guid.NewGuid();
        public string Name { get; }
        public string Owner { get; init; }
        public DateTime ExpiresAt { get; private set; }
        public bool IsExpired => DateTime.Now >= ExpiresAt;

        public string Token { get; } = Guid.NewGuid().ToString();

        public abstract void Dispose();

        public void UpdateExpiration()
        {
            ExpiresAt = DateTime.Now + _expirationTimeout;
        }
    }
}