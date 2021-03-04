using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using pro.savel.KafkaRestProxy.Consumer.Contract;
using pro.savel.KafkaRestProxy.Consumer.Requests;

namespace pro.savel.KafkaRestProxy.Consumer
{
    [ApiController]
    [Route("consumers")]
    public class ConsumerController : ControllerBase
    {
        private readonly ConsumerService _consumerService;

        public ConsumerController(ConsumerService consumerService)
        {
            _consumerService = consumerService;
        }

        [HttpGet]
        public ICollection<Contract.Consumer> ListConsumers()
        {
            return _consumerService.ListConsumers();
        }

        [HttpPost]
        [ProducesResponseType(StatusCodes.Status201Created, Type = typeof(Contract.Consumer))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public IActionResult CreateConsumer([Required] CreateConsumerRequest request)
        {
            var consumer = _consumerService.CreateConsumer(request);

            if (consumer == null) return BadRequest();
            return CreatedAtAction(nameof(GetConsumer), new {consumerId = consumer.Id}, consumer);
        }

        [HttpGet("{consumerId}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public Contract.Consumer GetConsumer(Guid consumerId)
        {
            return _consumerService.GetConsumer(consumerId);
        }

        [HttpDelete("{consumerId}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IActionResult RemoveConsumer(Guid consumerId)
        {
            _consumerService.RemoveConsumer(consumerId);

            return NoContent();
        }

        [HttpPost("{consumerId}/assignment")]
        [HttpPut("{consumerId}/assignment")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public ActionResult<IEnumerable<TopicPartition>> AssignConsumer(Guid consumerId,
            [Required] AssignConsumerRequest request)
        {
            if (consumerId != request.ConsumerId)
                return BadRequest("Consumer Id does not match provided data.");

            var result = _consumerService.AssignConsumer(request);

            return Ok(result);
        }

        [HttpGet("{consumerId}/assignment")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IEnumerable<TopicPartition> GetConsumerAssignment(Guid consumerId)
        {
            return _consumerService.GetConsumerAssignment(consumerId);
        }

        [HttpGet("{consumerId}/messages")]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(IEnumerable<ConsumerMessage>))]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IEnumerable<ConsumerMessage> Consume(Guid consumerId, [Range(0, 1000000)] int? timeout)
        {
            var result = _consumerService.Consume(consumerId, timeout);

            return result;
        }

        [HttpGet("{consumerId}/offsets")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public ICollection<WatermarkOffsets> GetWatermarkOffsets(Guid consumerId, int timeout)
        {
            var result = _consumerService.QueryWatermarkOffsets(consumerId, timeout);

            return result;
        }
    }
}