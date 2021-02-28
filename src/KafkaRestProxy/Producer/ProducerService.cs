using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using pro.savel.KafkaRestProxy.Producer.Contract;
using pro.savel.KafkaRestProxy.Producer.Requests;

namespace pro.savel.KafkaRestProxy.Producer
{
    public class ProducerService : IDisposable
    {
        private readonly IProducer<string, string> _producer;

        public ProducerService(ProducerConfig config)
        {
            _producer = new ProducerBuilder<string, string>(config).Build();
        }

        public void Dispose()
        {
            _producer.Dispose();
        }

        public async Task<DeliveryResult> PostMessage(string topic, PostMessageRequest request)
        {
            var producerMessage = ProducerMapper.Map(request);

            var producerDeliveryResult = await _producer.ProduceAsync(topic, producerMessage);

            var deliveryResult = ProducerMapper.Map(producerDeliveryResult);

            return deliveryResult;
        }
    }
}