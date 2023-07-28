﻿// Copyright 2023 Sergey Savelev
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Confluent.SchemaRegistry;
using Microsoft.Extensions.Options;

namespace SergeSavel.KafkaRestProxy.SchemaRegistry;

public class SchemaRegistryService : IDisposable
{
    public SchemaRegistryService(IOptions<SchemaRegistryConfig> configOptions)
    {
        var config = configOptions.Value;
        if (config.Url == null) return;
        Client = new CachedSchemaRegistryClient(config);
    }

    public ISchemaRegistryClient Client { get; }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing) Client?.Dispose();
    }
}