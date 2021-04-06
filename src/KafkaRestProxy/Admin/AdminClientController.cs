// Copyright 2021 Sergey Savelev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
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
        [ProducesResponseType(StatusCodes.Status201Created)]
        public async Task<ActionResult<bool>> CreateTopicAsync(CreateTopicRequest request,
            [Range(0, int.MaxValue)] int timeout)
        {
            await _adminClientService.CreateTopic(request, timeout);

            return CreatedAtAction(nameof(GetTopicMetadata), new {topic = request.Name, timeout}, true);
        }

        [HttpGet("topics/{topic}")]
        public TopicMetadata GetTopicMetadata(string topic, bool verbose,
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
        public BrokerMetadata GetBrokerMetadata(int brokerId, [Range(0, int.MaxValue)] int timeout)
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