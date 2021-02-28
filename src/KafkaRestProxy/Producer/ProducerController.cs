using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using pro.savel.KafkaRestProxy.Producer.Contract;
using pro.savel.KafkaRestProxy.Producer.Requests;

namespace pro.savel.KafkaRestProxy.Producer
{
    [ApiController]
    [Route("producer")]
    public class ProducerController : ControllerBase
    {
        private readonly ProducerService _producerService;

        public ProducerController(ProducerService producerService)
        {
            _producerService = producerService;
        }

        [HttpPost("{topic}")]
        public async Task<ActionResult<DeliveryResult>> PostMessage(string topic, PostMessageRequest request)
        {
            var result = await _producerService.PostMessage(topic, request);

            return Ok(result);
        }
    }
}