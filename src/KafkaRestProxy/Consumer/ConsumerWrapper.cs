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

namespace SergeSavel.KafkaRestProxy.Consumer
{
    public class ConsumerWrapper : IDisposable
    {
        private static readonly IDeserializer<string> KeyDeserializer = Deserializers.Utf8;

        private static readonly IDeserializer<string> ValueDeserializer = Deserializers.Utf8;

        private readonly TimeSpan _expirationTimeout;

        public ConsumerWrapper(IDictionary<string, string> consumerConfig, TimeSpan expirationTimeout)
        {
            _expirationTimeout = expirationTimeout;

            UpdateExpiration();

            Consumer = new ConsumerBuilder<string, string>(consumerConfig)
                .SetKeyDeserializer(KeyDeserializer)
                .SetValueDeserializer(ValueDeserializer)
                .Build();
        }

        public IConsumer<string, string> Consumer { get; }

        public Guid Id { get; } = Guid.NewGuid();

        public DateTime ExpiresAt { get; private set; }

        public bool IsExpired => DateTime.Now >= ExpiresAt;

        public string Creator { get; init; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void UpdateExpiration()
        {
            ExpiresAt = DateTime.Now + _expirationTimeout;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing) Consumer.Dispose();
        }
    }
}