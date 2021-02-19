using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using pro.savel.KafkaRestProxy.Entities;
using pro.savel.KafkaRestProxy.Services;

namespace pro.savel.KafkaRestProxy.Controllers
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
        public async Task<ActionResult<DeliveryResult>> PostMessage(string topic, Message message)
        {
            var result = await _producerService.PostMessage(topic, message);

            return Ok(result);
        }
    }
}