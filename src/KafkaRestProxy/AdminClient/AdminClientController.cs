using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using pro.savel.KafkaRestProxy.AdminClient.Requests;
using pro.savel.KafkaRestProxy.AdminClient.Responses;

namespace pro.savel.KafkaRestProxy.AdminClient
{
    [ApiController]
    [Route("admin")]
    public class AdminClientController : ControllerBase
    {
        private readonly AdminClientService _adminClientService;

        public AdminClientController(AdminClientService adminClientService)
        {
            _adminClientService = adminClientService;
        }

        [HttpGet("metadata")]
        public Metadata GetMetadata()
        {
            return _adminClientService.GetMetadata();
        }

        [HttpGet("metadata/topics")]
        public IEnumerable<TopicMetadata> GetTopicsMetadata()
        {
            var result = _adminClientService.GetTopicsMetadata();

            return result;
        }

        [HttpGet("metadata/topics/{topic}")]
        public ActionResult<TopicMetadata> GetTopicMetadata(string topic)
        {
            var result = _adminClientService.GetTopicMetadata(topic);

            if (result == null) return NotFound("Topic not found.");

            return result;
        }

        [HttpPost("metadata/topics")]
        public async Task<IActionResult> CreateTopic(CreateTopicRequest request)
        {
            await _adminClientService.CreateTopic(request.Name, request.NumPartitions, request.ReplicationFactor);

            return CreatedAtAction(nameof(GetTopicMetadata), new {topic = request.Name}, null);
        }

        [HttpGet("metadata/brokers")]
        public IEnumerable<BrokerMetadata> GetBrokersMetadata()
        {
            var result = _adminClientService.GetBrokersMetadata();

            return result;
        }

        [HttpGet("metadata/brokers/{brokerId}")]
        public ActionResult<BrokerMetadata> GetBrokerMetadata(int brokerId)
        {
            var result = _adminClientService.GetBrokerMetadata(brokerId);

            if (result == null) return NotFound("Broker not found.");

            return result;
        }
    }
}