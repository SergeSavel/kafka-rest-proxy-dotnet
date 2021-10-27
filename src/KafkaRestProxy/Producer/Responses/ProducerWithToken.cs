using System;

namespace SergeSavel.KafkaRestProxy.Producer.Responses
{
    public class ProducerWithToken : Producer
    {
        public Guid Token { get; init; }
    }
}