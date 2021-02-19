using System.Collections.Generic;

namespace pro.savel.KafkaRestProxy.Entities
{
    public class Message
    {
        public string Key { get; init; }
        public string Value { get; init; }
        public IDictionary<string, string> Headers { get; init; }
    }
}