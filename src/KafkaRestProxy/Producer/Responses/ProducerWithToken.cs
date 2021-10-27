namespace SergeSavel.KafkaRestProxy.Producer.Responses
{
    public class ProducerWithToken : Producer
    {
        public string Token { get; init; }
    }
}