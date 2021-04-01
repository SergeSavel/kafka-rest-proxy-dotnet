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

        [HttpGet]
        public Metadata GetMetadata(bool verbose)
        {
            return _adminClientService.GetMetadata(verbose);
        }

        [HttpGet("topics")]
        public TopicsMetadata GetTopicsMetadata(bool verbose)
        {
            return _adminClientService.GetTopicsMetadata(verbose);
        }

        [HttpPost("topics")]
        public async Task<ActionResult<TopicMetadata>> CreateTopicAsync(CreateTopicRequest request)
        {
            var result =
                await _adminClientService.CreateTopic(request);

            return CreatedAtAction(nameof(GetTopicMetadata), new {topic = result.Topic}, result);
        }

        [HttpGet("topics/{topic}")]
        public ActionResult<TopicMetadata> GetTopicMetadata(string topic, bool verbose)
        {
            return _adminClientService.GetTopicMetadata(topic, verbose);
        }

        [HttpGet("topics/{topic}/config")]
        public async Task<ResourceConfig> GetTopicConfigAsync(string topic)
        {
            return await _adminClientService.GetTopicConfigAsync(topic);
        }

        [HttpGet("brokers")]
        public BrokersMetadata GetBrokersMetadata()
        {
            return _adminClientService.GetBrokersMetadata();
        }

        [HttpGet("brokers/{brokerId}")]
        public ActionResult<BrokerMetadata> GetBrokerMetadata(int brokerId)
        {
            return _adminClientService.GetBrokerMetadata(brokerId);
        }

        [HttpGet("brokers/{brokerId}/config")]
        public async Task<ResourceConfig> GetTBrokerConfigAsync(int brokerId)
        {
            return await _adminClientService.GetBrokerConfigAsync(brokerId);
        }
    }
}