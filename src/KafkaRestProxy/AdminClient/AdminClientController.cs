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

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SergeSavel.KafkaRestProxy.AdminClient.Requests;
using SergeSavel.KafkaRestProxy.AdminClient.Responses;

namespace SergeSavel.KafkaRestProxy.AdminClient
{
    [ApiController]
    [Route("admin")]
    [Authorize]
    public class AdminClientController : ControllerBase
    {
        private readonly AdminClientService _service;

        public AdminClientController(AdminClientService service)
        {
            _service = service;
        }

        [HttpGet]
        public ICollection<Responses.AdminClient> ListClients()
        {
            return _service.ListClients();
        }

        [HttpGet("{clientId:guid}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public Responses.AdminClient GetClient(Guid clientId)
        {
            return _service.GetClient(clientId);
        }

        [HttpPost]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public ActionResult<AdminClientWithToken> CreateClient([Required] CreateAdminClientRequest request)
        {
            var client = _service.CreateClient(request, User.Identity?.Name);
            return CreatedAtAction(nameof(GetClient), new { clientId = client.Id }, client);
        }

        [HttpDelete("{clientId:guid}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public IActionResult RemoveClient(Guid clientId, Guid token)
        {
            _service.RemoveClient(clientId, token);
            return NoContent();
        }

        [HttpGet("{clientId:guid}/metadata")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public Metadata GetMetadata(Guid clientId, Guid token, [Range(0, int.MaxValue)] int timeoutMs)
        {
            return _service.GetMetadata(clientId, token, timeoutMs);
        }

        [HttpGet("{clientId:guid}/brokers")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public BrokersMetadata GetBrokersMetadata(Guid clientId, Guid token, [Range(0, int.MaxValue)] int timeoutMs)
        {
            return _service.GetBrokersMetadata(clientId, token, timeoutMs);
        }

        [HttpGet("{clientId:guid}/brokers/{brokerId:int}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public BrokerMetadata GetBrokerMetadata(Guid clientId, Guid token, int brokerId,
            [Range(0, int.MaxValue)] int timeoutMs)
        {
            return _service.GetBrokerMetadata(clientId, token, brokerId, timeoutMs);
        }

        [HttpGet("{clientId:guid}/topics")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public TopicsMetadata GetTopicsMetadata(Guid clientId, Guid token, [Range(0, int.MaxValue)] int timeoutMs)
        {
            return _service.GetTopicsMetadata(clientId, token, timeoutMs);
        }

        [HttpGet("{clientId:guid}/topics/{topic}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public TopicMetadata GetTopicMetadata(Guid clientId, Guid token, string topic,
            [Range(0, int.MaxValue)] int timeoutMs)
        {
            return _service.GetTopicMetadata(clientId, token, topic, timeoutMs);
        }

        [HttpPost("{clientId:guid}/topics")]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<ActionResult<bool>> CreateTopicAsync(Guid clientId, Guid token, CreateTopicRequest request,
            [Range(0, int.MaxValue)] int timeoutMs)
        {
            await _service.CreateTopicAsync(clientId, token, request, timeoutMs);
            return CreatedAtAction(nameof(GetTopicMetadata), new { topic = request.Topic, timeout = timeoutMs }, true);
        }

        [HttpGet("{clientId:guid}/topics/{topic}/config")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<ResourceConfig> GetTopicConfigAsync(Guid clientId, Guid token, string topic,
            [Range(0, int.MaxValue)] int timeoutMs)
        {
            return await _service.GetTopicConfigAsync(clientId, token, topic, timeoutMs);
        }

        [HttpGet("{clientId:guid}/brokers/{brokerId:int}/config")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        public async Task<ResourceConfig> GetBrokerConfigAsync(Guid clientId, Guid token, int brokerId,
            [Range(0, int.MaxValue)] int timeoutMs)
        {
            return await _service.GetBrokerConfigAsync(clientId, token, brokerId, timeoutMs);
        }
    }
}