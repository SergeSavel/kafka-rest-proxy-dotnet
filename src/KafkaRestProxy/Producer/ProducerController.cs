using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SergeSavel.KafkaRestProxy.Producer.Contract;
using SergeSavel.KafkaRestProxy.Producer.Requests;

namespace SergeSavel.KafkaRestProxy.Producer
{
    [ApiController]
    [Route("producer")]
    [Authorize]
    public class ProducerController : ControllerBase
    {
        private readonly ProducerService _producerService;

        public ProducerController(ProducerService producerService)
        {
            _producerService = producerService;
        }

        [HttpPost("produce")]
        public async Task<ActionResult<DeliveryResult>> PostMessage([Required] string topic,
            [Range(0, int.MaxValue)] int? partition, [Required] PostMessageRequest request)
        {
            var result = await _producerService.PostMessage(topic, partition, request);

            return Ok(result);
        }
    }
}