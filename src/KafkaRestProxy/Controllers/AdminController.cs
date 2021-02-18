using System.Collections.Generic;
using Confluent.Kafka;
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

        [HttpGet("metadata")]
        public Metadata GetMetadata()
        {
            return _adminClientService.GetMetadata();
        }
        
        [HttpGet("metadata/{topic}")]
        public TopicMetadata GetTopicMetadata(string topic)
        {
            return _adminClientService.GetTopicMetadata(topic);
        }
    }
}