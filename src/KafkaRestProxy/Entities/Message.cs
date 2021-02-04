using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace pro.savel.KafkaRestProxy.Entities
{
    public class Message
    {
        [JsonPropertyName("k")] public string Key { get; init; }

        [JsonPropertyName("h")] public IDictionary<string, string> Headers { get; init; }

        [JsonPropertyName("v")] public string Value { get; init; }
    }
}