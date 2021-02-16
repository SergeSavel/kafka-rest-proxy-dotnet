using Microsoft.AspNetCore.Mvc;
using pro.savel.KafkaRestProxy.Entities;
using pro.savel.KafkaRestProxy.Services;

namespace pro.savel.KafkaRestProxy.Controllers
{
    [ApiController]
    [Route("admin")]
    public class AdminController : ControllerBase
    {
        private readonly AdminClientService _adminClientService;

        public AdminController(AdminClientService adminClientService)
        {
            _adminClientService = adminClientService;
        }

        [HttpGet("{topic}")]
        public TopicInfo GetTopicInfo(string topic)
        {
            return _adminClientService.GetTopicInfo(topic);
        }
    }
}