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

using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SergeSavel.KafkaRestProxy.Common
{
    public abstract class ClientCleaner<TClientWrapper> : BackgroundService where TClientWrapper : ClientWrapper
    {
        private const long PeriodMs = 10000;

        private readonly ILogger _logger;
        private readonly ClientProvider<TClientWrapper> _provider;

        public ClientCleaner(ILogger logger, ClientProvider<TClientWrapper> provider)
        {
            _logger = logger;
            _provider = provider;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var stopwatch = Stopwatch.StartNew();

                _provider.RemoveExpiredItems();

                var remainingMs = PeriodMs - stopwatch.ElapsedMilliseconds;
                if (remainingMs > 0)
                    await Task.Delay((int)remainingMs, stoppingToken);
            }
        }
    }
}