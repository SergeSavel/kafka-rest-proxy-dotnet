using System.ComponentModel.DataAnnotations;
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
        public Metadata GetMetadata(bool verbose, [Range(0, int.MaxValue)] int timeout)
        {
            return _adminClientService.GetMetadata(verbose, timeout);
        }

        [HttpGet("topics")]
        public TopicsMetadata GetTopicsMetadata(bool verbose, [Range(0, int.MaxValue)] int timeout)
        {
            return _adminClientService.GetTopicsMetadata(verbose, timeout);
        }

        [HttpPost("topics")]
        public async Task<ActionResult<TopicMetadata>> CreateTopicAsync(CreateTopicRequest request,
            [Range(0, int.MaxValue)] int timeout)
        {
            await _adminClientService.CreateTopic(request, timeout);

            return CreatedAtAction(nameof(GetTopicMetadata), new {topic = request.Name, timeout}, null);
        }

        [HttpGet("topics/{topic}")]
        public ActionResult<TopicMetadata> GetTopicMetadata(string topic, bool verbose,
            [Range(0, int.MaxValue)] int timeout)
        {
            return _adminClientService.GetTopicMetadata(topic, verbose, timeout);
        }

        [HttpGet("topics/{topic}/config")]
        public async Task<ResourceConfig> GetTopicConfigAsync(string topic, [Range(0, int.MaxValue)] int timeout)
        {
            return await _adminClientService.GetTopicConfigAsync(topic, timeout);
        }

        [HttpGet("brokers")]
        public BrokersMetadata GetBrokersMetadata([Range(0, int.MaxValue)] int timeout)
        {
            return _adminClientService.GetBrokersMetadata(timeout);
        }

        [HttpGet("brokers/{brokerId}")]
        public ActionResult<BrokerMetadata> GetBrokerMetadata(int brokerId, [Range(0, int.MaxValue)] int timeout)
        {
            return _adminClientService.GetBrokerMetadata(brokerId, timeout);
        }

        [HttpGet("brokers/{brokerId}/config")]
        public async Task<ResourceConfig> GetTBrokerConfigAsync(int brokerId, [Range(0, int.MaxValue)] int timeout)
        {
            return await _adminClientService.GetBrokerConfigAsync(brokerId, timeout);
        }
    }
}