using System.Collections.Generic;

namespace SergeSavel.KafkaRestProxy.Admin.Contract
{
    public class ResourceConfig
    {
        public string ResourceType { get; init; }

        public string ResourceName { get; init; }

        public IDictionary<string, ConfigEntryValue> Entries { get; init; }

        public class ConfigEntryValue
        {
            public string Value { get; init; }

            public bool IsDefault { get; init; }

            public bool IsReadOnly { get; init; }

            public bool IsSensitive { get; init; }
        }
    }
}