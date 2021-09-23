// Copyright 2021 Sergey Savelev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using SergeSavel.KafkaRestProxy.Producer.Contract;
using SergeSavel.KafkaRestProxy.Producer.Exceptions;
using SergeSavel.KafkaRestProxy.Producer.Requests;

namespace SergeSavel.KafkaRestProxy.Producer
{
    public class ProducerService : IDisposable
    {
        private readonly IProducer<string, string> _producer;

        public ProducerService(ProducerConfig config)
        {
            _producer = new ProducerBuilder<string, string>(config).Build();
        }

        public Handle Handle => _producer.Handle;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public async Task<DeliveryResult> PostMessage(string topic, int? partition, PostMessageRequest request)
        {
            var producerMessage = ProducerMapper.Map(request);

            DeliveryResult<string, string> producerDeliveryResult;
            try
            {
                producerDeliveryResult = partition.HasValue
                    ? await _producer.ProduceAsync(new TopicPartition(topic, partition.Value), producerMessage)
                    : await _producer.ProduceAsync(topic, producerMessage);
            }
            catch (KafkaException e)
            {
                throw new ProduceException(e);
            }

            var deliveryResult = ProducerMapper.Map(producerDeliveryResult);

            return deliveryResult;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _producer.Flush();
                _producer.Dispose();
            }
        }
    }
}