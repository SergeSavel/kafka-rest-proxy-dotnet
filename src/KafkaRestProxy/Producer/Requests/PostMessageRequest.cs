using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace pro.savel.KafkaRestProxy.Producer.Requests
{
    public class PostMessageRequest
    {
        public string Key { get; init; }

        [Required] public string Value { get; init; }

        public IDictionary<string, string> Headers { get; init; }
    }
}