using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SergeSavel.KafkaRestProxy.Consumer.Contract;
using SergeSavel.KafkaRestProxy.Consumer.Requests;

namespace SergeSavel.KafkaRestProxy.Consumer
{
    [ApiController]
    [Route("consumers")]
    [Authorize]
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
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public ActionResult<Contract.Consumer> CreateConsumer([Required] CreateConsumerRequest request)
        {
            var consumer = _consumerService.CreateConsumer(request, User.Identity?.Name);
            return CreatedAtAction(nameof(GetConsumer), new {consumerId = consumer.Id}, consumer);
        }

        [HttpDelete("{consumerId}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IActionResult RemoveConsumer(Guid consumerId)
        {
            _consumerService.RemoveConsumer(consumerId);

            return NoContent();
        }

        [HttpGet("{consumerId}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public Contract.Consumer GetConsumer(Guid consumerId)
        {
            return _consumerService.GetConsumer(consumerId);
        }

        [HttpPost("{consumerId}/assignment")]
        [HttpPut("{consumerId}/assignment")]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public ActionResult<ICollection<TopicPartition>> AssignConsumer(Guid consumerId,
            [Required] AssignConsumerRequest request)
        {
            if (consumerId != request.ConsumerId)
                return BadRequest("Consumer Id does not match provided data.");

            var consumerAssignment = _consumerService.AssignConsumer(request);

            return CreatedAtAction(nameof(GetConsumerAssignment), new {consumerId}, consumerAssignment);
        }

        [HttpGet("{consumerId}/assignment")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IEnumerable<TopicPartition> GetConsumerAssignment(Guid consumerId)
        {
            return _consumerService.GetConsumerAssignment(consumerId);
        }

        [HttpGet("{consumerId}/consume")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public ConsumerMessage Consume(Guid consumerId, [Range(0, int.MaxValue)] int? timeout)
        {
            return _consumerService.Consume(consumerId, timeout);
        }

        [Obsolete]
        [HttpGet("{consumerId}/messages")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IEnumerable<ConsumerMessage> ConsumeMultiple(Guid consumerId, [Range(0, int.MaxValue)] int? timeout,
            [Range(1, int.MaxValue)] int? limit)
        {
            return _consumerService.ConsumeMultiple(consumerId, timeout, limit);
        }

        [HttpGet("{consumerId}/offsets")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public PartitionOffsets GetPartitionOffsets(Guid consumerId, [Required] string topic,
            [Required] [Range(0, int.MaxValue)] int partition, [Range(0, int.MaxValue)] int? timeout)
        {
            return _consumerService.GetPartitionOffsets(consumerId, topic, partition, timeout);
        }
    }
}