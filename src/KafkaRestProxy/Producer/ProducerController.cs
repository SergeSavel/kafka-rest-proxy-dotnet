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
using SergeSavel.KafkaRestProxy.Producer.Requests;
using SergeSavel.KafkaRestProxy.Producer.Responses;

namespace SergeSavel.KafkaRestProxy.Producer
{
    [ApiController]
    [Route("producer")]
    [Produces("application/json")]
    [Authorize]
    public class ProducerController : ControllerBase
    {
        private readonly ProducerService _service;

        public ProducerController(ProducerService service)
        {
            _service = service;
        }

        /// <summary> List alive producer instances.</summary>
        /// <returns>Producer instances list (without tokens).</returns>
        /// <response code="200">Returns producer instances list (without tokens).</response>
        [HttpGet]
        public ICollection<Responses.Producer> ListProducers()
        {
            return _service.ListProducers();
        }

        /// <summary>Get producer instance info by Id.</summary>
        /// <param name="producerId">Producer instance Id.</param>
        /// <returns>Instance info (without token).</returns>
        /// <response code="200">Returns producer instances info (without token).</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance not found.</response>
        [HttpGet("{producerId:guid}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public Responses.Producer GetProducer(Guid producerId)
        {
            return _service.GetProducer(producerId);
        }

        /// <summary>Create new producer instance.</summary>
        /// <param name="request">New instance config.</param>
        /// <returns>New instance info (with token)</returns>
        /// <response code="200">Returns new instance info (with token).</response>
        /// <response code="400">Invalid instance config.</response>
        [HttpPost]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public ActionResult<ProducerWithToken> CreateProducer([Required] CreateProducerRequest request)
        {
            var producer = _service.CreateProducer(request, User.Identity?.Name);
            return CreatedAtAction(nameof(GetProducer), new { producerId = producer.Id }, producer);
        }

        /// <summary>Remove producer instance.</summary>
        /// <param name="producerId">Producer instance Id.</param>
        /// <param name="token">Security token obtained while creating current instance.</param>
        /// <response code="204">Instance successfully removed.</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance not found.</response>
        [HttpDelete("{producerId:guid}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IActionResult RemoveProducer(Guid producerId, string token)
        {
            _service.RemoveProducer(producerId, token);
            return NoContent();
        }

        /// <summary>Post new message.</summary>
        /// <param name="producerId">Producer instance Id.</param>
        /// <param name="token">Security token obtained while creating current instance.</param>
        /// <param name="topic">Topic.</param>
        /// <param name="partition">Partition (optional).</param>
        /// <param name="request">New message.</param>
        /// <returns>Delivery result.</returns>
        /// <response code="200">Returns delivery result.</response>
        /// <response code="400">An error occured during data serialization.</response>
        /// <response code="403">Invalid token.</response>
        /// <response code="404">Instance not found.</response>
        /// <response code="500">An error occured while posting the message.</response>
        [HttpPost("{producerId:guid}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<ActionResult<DeliveryResult>> ProduceAsync(Guid producerId, string token,
            [Required] string topic,
            [Range(0, int.MaxValue)] int? partition, [Required] PostMessageRequest request)
        {
            var result = await _service.ProduceAsync(producerId, token, topic, partition, request)
                .ConfigureAwait(false);
            return Ok(result);
        }
    }
}