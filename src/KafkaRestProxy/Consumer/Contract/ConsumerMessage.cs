using System.Collections.Generic;

namespace pro.savel.KafkaRestProxy.Consumer.Contract
{
    public class ConsumerMessage
    {
        public long? Timestamp { get; init; }
        public long? Offset { get; init; }
        public string Key { get; init; }
        public IDictionary<string, string> Headers { get; init; }
        public string Value { get; init; }
    }
}