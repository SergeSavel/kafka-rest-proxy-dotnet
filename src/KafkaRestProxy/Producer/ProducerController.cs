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
    [Authorize]
    public class ProducerController : ControllerBase
    {
        private readonly ProducerService _service;

        public ProducerController(ProducerService service)
        {
            _service = service;
        }

        [HttpGet]
        public ICollection<Responses.Producer> ListProducers()
        {
            return _service.ListProducers();
        }

        [HttpGet("{producerId:guid}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public Responses.Producer GetProducer(Guid producerId)
        {
            return _service.GetProducer(producerId);
        }

        [HttpPost]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public ActionResult<ProducerWithToken> CreateProducer([Required] CreateProducerRequest request)
        {
            var producer = _service.CreateProducer(request, User.Identity?.Name);
            return CreatedAtAction(nameof(GetProducer), new { producerId = producer.Id }, producer);
        }

        [HttpDelete("{producerId:guid}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status403Forbidden)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IActionResult RemoveProducer(Guid producerId, Guid token)
        {
            _service.RemoveProducer(producerId, token);
            return NoContent();
        }

        [HttpPost("{producerId:guid}")]
        public async Task<ActionResult<DeliveryResult>> PostMessage(Guid producerId, Guid token,
            [Required] string topic,
            [Range(0, int.MaxValue)] int? partition, [Required] PostMessageRequest request)
        {
            var result = await _service.ProduceAsync(producerId, token, topic, partition, request)
                .ConfigureAwait(false);

            return Ok(result);
        }
    }
}