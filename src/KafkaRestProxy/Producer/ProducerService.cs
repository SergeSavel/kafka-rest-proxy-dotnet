using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using pro.savel.KafkaRestProxy.Producer.Contract;

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

        public async Task<DeliveryResult> PostMessage(string topic, Message message)
        {
            var producerMessage = ProducerMapper.MapMessage(message);

            var producerDeliveryResult = await _producer.ProduceAsync(topic, producerMessage);

            var deliveryResult = ProducerMapper.MapDeliveryResult(producerDeliveryResult);

            return deliveryResult;
        }
    }
}