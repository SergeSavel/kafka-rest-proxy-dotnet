// Copyright 2023 Sergey Savelev
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

using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using SergeSavel.KafkaRestProxy.Common.Extensions;

namespace SergeSavel.KafkaRestProxy.Consumer;

public class StringAvroDeserializer : IDeserializer<string>
{
    private readonly IDeserializer<GenericRecord> _avroDeserializer;

    public StringAvroDeserializer(ISchemaRegistryClient schemaRegistryClient)
    {
        _avroDeserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient).AsSyncOverAsync();
    }

    public StringAvroDeserializer(ISchemaRegistryClient schemaRegistryClient,
        IEnumerable<KeyValuePair<string, string>> config)
    {
        _avroDeserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, config).AsSyncOverAsync();
    }

    public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        var genericRecord = _avroDeserializer.Deserialize(data, isNull, context);
        return genericRecord.AsXmlString();
    }
}