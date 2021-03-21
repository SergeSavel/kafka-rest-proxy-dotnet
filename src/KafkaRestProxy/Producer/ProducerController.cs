using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using SergeSavel.KafkaRestProxy.Producer.Contract;
using SergeSavel.KafkaRestProxy.Producer.Requests;

namespace SergeSavel.KafkaRestProxy.Producer
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
        public async Task<ActionResult<DeliveryResult>> PostMessage(string topic, [Required] PostMessageRequest request)
        {
            var result = await _producerService.PostMessage(topic, request);

            return Ok(result);
        }
    }
}