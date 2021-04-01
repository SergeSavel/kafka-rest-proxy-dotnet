using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SergeSavel.KafkaRestProxy.Admin.Contract;
using SergeSavel.KafkaRestProxy.Admin.Requests;

namespace SergeSavel.KafkaRestProxy.Admin
{
    [ApiController]
    [Route("admin")]
    [Authorize]
    public class AdminClientController : ControllerBase
    {
        private readonly AdminClientService _adminClientService;

        public AdminClientController(AdminClientService adminClientService)
        {
            _adminClientService = adminClientService;
        }

        [HttpGet("metadata")]
        public Metadata GetMetadata(bool verbose)
        {
            return _adminClientService.GetMetadata(verbose);
        }

        [HttpGet("metadata/topics")]
        public TopicsMetadata GetTopicsMetadata(bool verbose)
        {
            return _adminClientService.GetTopicsMetadata(verbose);
        }

        [HttpGet("metadata/topics/{topic}")]
        public ActionResult<TopicMetadata> GetTopicMetadata(string topic, bool verbose)
        {
            return _adminClientService.GetTopicMetadata(topic, verbose);
        }

        [HttpPost("metadata/topics")]
        public async Task<ActionResult<TopicMetadata>> CreateTopic(CreateTopicRequest request)
        {
            var result =
                await _adminClientService.CreateTopic(request);

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