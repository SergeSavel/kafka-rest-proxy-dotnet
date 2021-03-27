using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using SergeSavel.KafkaRestProxy.Admin.Contract;
using SergeSavel.KafkaRestProxy.Admin.Requests;
using SergeSavel.KafkaRestProxy.Common.Authentication;

namespace SergeSavel.KafkaRestProxy.Admin
{
    [ApiController]
    [Route("admin")]
    [BasicAuth]
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
        public TopicsMetadata GetTopicsMetadata()
        {
            return _adminClientService.GetTopicsMetadata();
        }

        [HttpGet("metadata/topics/{topic}")]
        public ActionResult<TopicMetadata> GetTopicMetadata(string topic)
        {
            return _adminClientService.GetTopicMetadata(topic);
        }

        [HttpPost("metadata/topics")]
        public async Task<ActionResult<TopicMetadata>> CreateTopic(CreateTopicRequest request)
        {
            var result =
                await _adminClientService.CreateTopic(request.Name, request.NumPartitions, request.ReplicationFactor);

            return CreatedAtAction(nameof(GetTopicMetadata), new {topic = result.Topic}, result);
        }

        [HttpGet("metadata/brokers")]
        public BrokersMetadata GetBrokersMetadata()
        {
            return _adminClientService.GetBrokersMetadata();
        }

        [HttpGet("metadata/brokers/{brokerId}")]
        public ActionResult<BrokerMetadata> GetBrokerMetadata(int brokerId)
        {
            return _adminClientService.GetBrokerMetadata(brokerId);
        }
    }
}