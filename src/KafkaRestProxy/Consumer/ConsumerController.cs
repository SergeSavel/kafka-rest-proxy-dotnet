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
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SergeSavel.KafkaRestProxy.Common.Responses;
using SergeSavel.KafkaRestProxy.Consumer.Requests;
using SergeSavel.KafkaRestProxy.Consumer.Responses;

namespace SergeSavel.KafkaRestProxy.Consumer;

[ApiController]
[Route("consumer")]
[Produces("application/json")]
[Authorize]
public class ConsumerController : ControllerBase
{
    private readonly ConsumerService _service;

    public ConsumerController(ConsumerService service)
    {
        _service = service;
    }

    /// <summary> List alive consumer instances.</summary>
    /// <returns>Consumer instances list (without tokens).</returns>
    /// <response code="200">Returns consumer instances list (without tokens).</response>
    [HttpGet]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public ICollection<Responses.Consumer> ListConsumers()
    {
        return _service.ListConsumers();
    }

    /// <summary>Get consumer instance info by Id.</summary>
    /// <param name="consumerId">Consumer instance Id.</param>
    /// <returns>Instance info (without token).</returns>
    /// <response code="200">Returns consumer instances info (without token).</response>
    /// <response code="404">Instance not found.</response>
    [HttpGet("{consumerId:guid}")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public Responses.Consumer GetConsumer(Guid consumerId)
    {
        return _service.GetConsumer(consumerId);
    }

    /// <summary>Create new consumer instance.</summary>
    /// <param name="request">New instance config.</param>
    /// <returns>New instance info (with token)</returns>
    /// <response code="200">Returns new instance info (with token).</response>
    /// <response code="400">Invalid instance config.</response>
    [HttpPost]
    [ProducesResponseType(StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public ActionResult<Responses.Consumer> CreateConsumer([Required] CreateConsumerRequest request)
    {
        var consumer = _service.CreateConsumer(request, User.Identity?.Name);
        return CreatedAtAction(nameof(GetConsumer), new { consumerId = consumer.Id }, consumer);
    }

    /// <summary>Remove consumer instance.</summary>
    /// <param name="consumerId">Consumer instance Id.</param>
    /// <param name="token">Security token obtained while creating current instance.</param>
    /// <response code="204">Instance successfully removed.</response>
    /// <response code="403">Invalid token.</response>
    /// <response code="404">Instance not found.</response>
    [HttpDelete("{consumerId:guid}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public IActionResult RemoveConsumer(Guid consumerId, [Required] string token)
    {
        _service.RemoveConsumer(consumerId, token);
        return NoContent();
    }

    /// <summary>
    ///     Sets the current set of assigned partitions
    ///     (the set of partitions the consumer will consume from).
    /// </summary>
    /// <param name="consumerId">Consumer instance Id.</param>
    /// <param name="token">Security token obtained while creating current instance.</param>
    /// <param name="request">The set of partitions (with offset) to consume from.</param>
    /// <returns>The current partition assignment.</returns>
    /// <response code="201">Returns the current partition assignment.</response>
    /// <response code="403">Invalid token.</response>
    /// <response code="404">Instance not found.</response>
    [HttpPost("{consumerId:guid}/assignment")]
    [ProducesResponseType(StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult<ICollection<TopicPartition>> AssignConsumer(Guid consumerId, [Required] string token,
        [Required] IEnumerable<TopicPartitionOffset> request)
    {
        var consumerAssignment = _service.AssignConsumer(consumerId, token, request);
        return CreatedAtAction(nameof(GetConsumerAssignment), new { consumerId, token }, consumerAssignment);
    }

    /// <summary>Gets the current partition assignment.</summary>
    /// <param name="consumerId">Consumer instance Id.</param>
    /// <param name="token">Security token obtained while creating current instance.</param>
    /// <returns>The current partition assignment.</returns>
    /// <response code="200">Returns the current partition assignment.</response>
    /// <response code="403">Invalid token.</response>
    /// <response code="404">Instance not found.</response>
    [HttpGet("{consumerId:guid}/assignment")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public IEnumerable<TopicPartition> GetConsumerAssignment(Guid consumerId, [Required] string token)
    {
        return _service.GetConsumerAssignment(consumerId, token);
    }

    /// <summary>
    ///     Poll for new messages / events.
    ///     Blocks until a consume result is available or the timeout period has elapsed.
    /// </summary>
    /// <param name="consumerId">Consumer instance Id.</param>
    /// <param name="token">Security token obtained while creating current instance.</param>
    /// <param name="timeout">(optional) The maximum period of time (ms) the call may block.</param>
    /// <returns>Consume result (new message, if exists).</returns>
    /// <response code="200">Returns consume result (new message).</response>
    /// <response code="204">No new messages.</response>
    /// <response code="403">Invalid token.</response>
    /// <response code="404">Instance not found.</response>
    /// <response code="500">Returns error details.</response>
    [HttpGet("{consumerId:guid}/consume")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public ActionResult<ConsumerMessage> Consume(Guid consumerId, [Required] string token,
        [Required] [Range(0, int.MaxValue)] int timeout)
    {
        var consumerMessage = _service.Consume(consumerId, token, TimeSpan.FromMilliseconds(timeout));
        if (consumerMessage == null) return NoContent();
        return new ActionResult<ConsumerMessage>(consumerMessage);
    }

    /// <summary>
    ///     Get the low (oldest available/beginning) and high (newest/end) offsets for the specified topic/partition.
    ///     If timeout is set, consumer will query cluster (this is a blocking call - always contacts
    ///     the cluster for the required information).
    ///     If timeout is not set, result will be obtained from cache.
    /// </summary>
    /// <param name="consumerId">Consumer instance Id.</param>
    /// <param name="token">Security token obtained while creating current instance.</param>
    /// <param name="topic">Topic.</param>
    /// <param name="partition">Partition.</param>
    /// <param name="timeout">(optional) The maximum period of time the call may block.</param>
    /// <returns>Requested watermark offsets.</returns>
    /// <response code="200">Returns requested watermark offsets.</response>
    /// <response code="403">Invalid token.</response>
    /// <response code="404">Instance not found.</response>
    /// <response code="500">Returns error details.</response>
    [HttpGet("{consumerId:guid}/offsets")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public PartitionOffsets GetPartitionOffsets(Guid consumerId, [Required] string token, [Required] string topic,
        [Required] [Range(0, int.MaxValue)] int partition, [Range(0, int.MaxValue)] int? timeout)
    {
        return _service.GetPartitionOffsets(consumerId, token, topic, partition,
            timeout.HasValue ? TimeSpan.FromMilliseconds(timeout.Value) : null);
    }

    /// <summary>Get metadata.</summary>
    /// <param name="consumerId">Consumer instance Id.</param>
    /// <param name="token">Security token obtained while creating current instance.</param>
    /// <param name="topic">(optional) Topic name.</param>
    /// <param name="timeout">Operation timeout (ms).</param>
    /// <returns>All cluster metadata.</returns>
    /// <response code="200">Returns cluster metadata.</response>
    /// <response code="403">Invalid token.</response>
    /// <response code="404">Instance not found.</response>
    /// <response code="500">Returns error details.</response>
    [HttpGet("{consumerId:guid}/metadata")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public Metadata GetMetadata(Guid consumerId, [Required] string token, string topic,
        [Required] [Range(0, int.MaxValue)] int timeout)
    {
        return topic == null
            ? _service.GetMetadata(consumerId, token, TimeSpan.FromMilliseconds(timeout))
            : _service.GetMetadata(consumerId, token, topic, TimeSpan.FromMilliseconds(timeout));
    }
}