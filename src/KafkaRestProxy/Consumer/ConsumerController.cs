using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using pro.savel.KafkaRestProxy.Consumer.Contract;

namespace pro.savel.KafkaRestProxy.Consumer
{
    [ApiController]
    [Route("consumer")]
    public class ConsumerController : ControllerBase
    {
        private readonly ConsumerService _consumerService;

        public ConsumerController(ConsumerService consumerService)
        {
            _consumerService = consumerService;
        }

        [HttpGet]
        public IEnumerable<Contract.Consumer> ListConsumers()
        {
            return _consumerService.ListConsumers();
        }

        [HttpPost]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(Contract.Consumer))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public IActionResult CreateConsumer(string topic, int partition, long? offset, int expirationTimeout,
            string groupId)
        {
            var consumer = _consumerService.CreateConsumer(topic, partition, offset, expirationTimeout, groupId);
            if (consumer == null) return BadRequest();
            return Ok(consumer);
        }

        [HttpGet("{consumerId}")]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(Contract.Consumer))]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IActionResult GetConsumer(Guid consumerId)
        {
            var consumer = _consumerService.GetConsumer(consumerId);

            if (consumer == null) return NotFound();
            return Ok(consumer);
        }

        [HttpDelete("{consumerId}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IActionResult RemoveConsumer(Guid consumerId)
        {
            return _consumerService.RemoveConsumer(consumerId) ? Ok() : NotFound();
        }

        [HttpGet("{consumerId}/consume")]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(ConsumerMessage))]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IActionResult Consume(Guid consumerId, int timeout = 0)
        {
            var consumerMessage = _consumerService.Consume(consumerId, timeout);

            if (consumerMessage == null) return NotFound();

            return Ok(consumerMessage);
        }
    }
}