using System.Collections.Generic;

namespace SergeSavel.KafkaRestProxy
{
    public class ProxyConfig
    {
        public const string SectionName = "Proxy";

        public bool IndentOutput { get; init; } = true;

        public IDictionary<string, string> Users { get; init; }
    }
}