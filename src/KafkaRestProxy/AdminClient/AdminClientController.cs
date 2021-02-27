﻿using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using pro.savel.KafkaRestProxy.AdminClient.Contract;

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

        [HttpGet("metadata/{topic}")]
        public ActionResult<TopicMetadata> GetTopicMetadata(string topic)
        {
            var result = _adminClientService.GetTopicMetadata(topic);

            if (result == null) return NotFound("Topic not found.");

            return result;
        }

        [HttpPost("topics")]
        public async Task<IActionResult> CreateTopic(CreateTopicRequest request)
        {
            await _adminClientService.CreateTopic(request.Name, request.NumPartitions, request.ReplicationFactor);

            return CreatedAtAction(nameof(GetMetadata), new {topic = request.Name}, null);
        }
    }
}