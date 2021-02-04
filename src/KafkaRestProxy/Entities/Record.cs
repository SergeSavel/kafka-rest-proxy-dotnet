using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace pro.savel.KafkaRestProxy.Entities
{
    public class Record
    {
        [JsonPropertyName("t")] public long Timestamp { get; init; }

        [JsonPropertyName("o")] public long Offset { get; init; }

        [JsonPropertyName("k")] public string Key { get; init; }

        [JsonPropertyName("h")] public IDictionary<string, string> Headers { get; init; }

        [JsonPropertyName("v")] public string Values { get; init; }
    }
}