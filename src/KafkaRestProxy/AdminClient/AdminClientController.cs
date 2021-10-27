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
    [Produces("application/json")]
    [Authorize]
    public class AdminClientController : ControllerBase
    {
        private readonly AdminClientService _service;

        public AdminClientController(AdminClientService service)
        {
            _service = service;
        }

        /// <summary> List alive admin client instances.</summary>
        /// <returns>Admin client instances list (without tokens).</returns>
        /// <response code="200">Returns admin client instances list (without tokens).</response>
        [HttpGet]
        public ICollection<Responses.AdminClient> ListClients()
        {
            return _service.ListClients();
        }

        /// <summary>Get admin client instance info by Id.</summary>
        /// <param name="clientId">Instance Id.</param>
        /// <returns>Instance info (without token).</returns>
        /// <response code="200">Returns admin client instances info (without token).</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance not found.</response>
        [HttpGet("{clientId:guid}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public Responses.AdminClient GetClient(Guid clientId)
        {
            return _service.GetClient(clientId);
        }

        /// <summary>Create new admin client instance.</summary>
        /// <param name="request">New instance config.</param>
        /// <returns>New instance info (with token)</returns>
        /// <response code="200">Returns new instance info (with token).</response>
        /// <response code="400">Invalid instance config.</response>
        [HttpPost]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public ActionResult<AdminClientWithToken> CreateClient([Required] CreateAdminClientRequest request)
        {
            var client = _service.CreateClient(request, User.Identity?.Name);
            return CreatedAtAction(nameof(GetClient), new { clientId = client.Id }, client);
        }

        /// <summary>Remove admin client instance.</summary>
        /// <param name="clientId">Admin client instance Id.</param>
        /// <param name="token">Security token obtained while creating current instance.</param>
        /// <response code="204">Instance successfully removed.</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance not found.</response>
        [HttpDelete("{clientId:guid}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IActionResult RemoveClient(Guid clientId, Guid token)
        {
            _service.RemoveClient(clientId, token);
            return NoContent();
        }

        /// <summary>Get cluster metadata.</summary>
        /// <param name="clientId">Admin client instance Id.</param>
        /// <param name="token">Security token obtained while creating current instance.</param>
        /// <param name="timeout">Operation timeout (ms).</param>
        /// <returns>All cluster metadata.</returns>
        /// <response code="200">Returns cluster metadata.</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance not found.</response>
        [HttpGet("{clientId:guid}/metadata")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public Metadata GetMetadata(Guid clientId, Guid token, [Range(0, int.MaxValue)] int timeout)
        {
            return _service.GetMetadata(clientId, token, timeout);
        }

        /// <summary>Get brokers metadata</summary>
        /// <param name="clientId">Admin client instance Id.</param>
        /// <param name="token">Security token obtained while creating current instance.</param>
        /// <param name="timeout">Operation timeout (ms).</param>
        /// <returns>Brokers metadata.</returns>
        /// <response code="200">Returns brokers metadata.</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance not found.</response>
        [HttpGet("{clientId:guid}/brokers")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public BrokersMetadata GetBrokersMetadata(Guid clientId, Guid token, [Range(0, int.MaxValue)] int timeout)
        {
            return _service.GetBrokersMetadata(clientId, token, timeout);
        }

        /// <summary>Get broker metadata.</summary>
        /// <param name="clientId">Admin client instance Id.</param>
        /// <param name="token">Security token obtained while creating current instance.</param>
        /// <param name="brokerId">Broker Id.</param>
        /// <param name="timeout">Operation timeout (ms).</param>
        /// <returns>Broker metadata.</returns>
        /// <response code="200">Returns broker metadata.</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance/broker not found.</response>
        [HttpGet("{clientId:guid}/brokers/{brokerId:int}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public BrokerMetadata GetBrokerMetadata(Guid clientId, Guid token, int brokerId,
            [Range(0, int.MaxValue)] int timeout)
        {
            return _service.GetBrokerMetadata(clientId, token, brokerId, timeout);
        }

        /// <summary>Get topics metadata.</summary>
        /// <param name="clientId">Admin client instance Id.</param>
        /// <param name="token">Security token obtained while creating current instance.</param>
        /// <param name="timeout">Operation timeout (ms).</param>
        /// <returns>Topics metadata.</returns>
        /// <response code="200">Returns topics metadata.</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance not found.</response>
        [HttpGet("{clientId:guid}/topics")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public TopicsMetadata GetTopicsMetadata(Guid clientId, Guid token, [Range(0, int.MaxValue)] int timeout)
        {
            return _service.GetTopicsMetadata(clientId, token, timeout);
        }

        /// <summary>Get topic metadata.</summary>
        /// <param name="clientId">Admin client instance Id.</param>
        /// <param name="token">Security token obtained while creating current instance.</param>
        /// <param name="topic">Topic name.</param>
        /// <param name="timeout">Operation timeout (ms).</param>
        /// <returns>Topic metadata.</returns>
        /// <response code="200">Returns broker metadata.</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance/topic not found.</response>
        [HttpGet("{clientId:guid}/topics/{topic}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public TopicMetadata GetTopicMetadata(Guid clientId, Guid token, string topic,
            [Range(0, int.MaxValue)] int timeout)
        {
            return _service.GetTopicMetadata(clientId, token, topic, timeout);
        }

        /// <summary></summary>
        /// <param name="clientId">Admin client instance Id.</param>
        /// <param name="token">Security token obtained while creating current instance.</param>
        /// <param name="request">New topic config.</param>
        /// <param name="timeout">Operation timeout (ms).</param>
        /// <response code="201">Topic successfully created.</response>
        /// <response code="400">Topic already exists.</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance not found.</response>
        /// <response code="500">An error occured while creating the topic.</response>
        [HttpPost("{clientId:guid}/topics")]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<ActionResult<bool>> CreateTopicAsync(Guid clientId, Guid token, CreateTopicRequest request,
            [Range(0, int.MaxValue)] int timeout)
        {
            await _service.CreateTopicAsync(clientId, token, request, timeout);
            return CreatedAtAction(nameof(GetTopicMetadata), new { topic = request.Topic, timeout }, true);
        }

        /// <summary>Get topic config.</summary>
        /// <param name="clientId">Admin client instance Id.</param>
        /// <param name="token">Security token obtained while creating current instance.</param>
        /// <param name="topic">Topic name.</param>
        /// <param name="timeout">Operation timeout (ms).</param>
        /// <returns>Topic config.</returns>
        /// <response code="200">Returns topic config.</response>
        /// <response code="400">Unsupported feature.</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance/topic not found.</response>
        /// <response code="500">An error occured while fetching topic config.</response>
        [HttpGet("{clientId:guid}/topics/{topic}/config")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ResourceConfig> GetTopicConfigAsync(Guid clientId, Guid token, string topic,
            [Range(0, int.MaxValue)] int timeout)
        {
            return await _service.GetTopicConfigAsync(clientId, token, topic, timeout);
        }

        /// <summary>Get broker config.</summary>
        /// <param name="clientId">Admin client instance Id.</param>
        /// <param name="token">Security token obtained while creating current instance.</param>
        /// <param name="brokerId">Broker Id.</param>
        /// <param name="timeout">Operation timeout (ms).</param>
        /// <returns>Broker config.</returns>
        /// <response code="200">Returns broker config.</response>
        /// <response code="400">Unsupported feature.</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance/broker not found.</response>
        /// <response code="500">An error occured while fetching broker config.</response>
        [HttpGet("{clientId:guid}/brokers/{brokerId:int}/config")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ResourceConfig> GetBrokerConfigAsync(Guid clientId, Guid token, int brokerId,
            [Range(0, int.MaxValue)] int timeout)
        {
            return await _service.GetBrokerConfigAsync(clientId, token, brokerId, timeout);
        }
    }
}