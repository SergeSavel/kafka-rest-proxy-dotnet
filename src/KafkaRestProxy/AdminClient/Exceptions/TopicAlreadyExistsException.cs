﻿using Microsoft.AspNetCore.Http;
using SergeSavel.KafkaRestProxy.Common.Exceptions;

namespace SergeSavel.KafkaRestProxy.AdminClient.Exceptions
{
    public class TopicAlreadyExistsException : HttpResponseException
    {
        public TopicAlreadyExistsException(string topic)
        {
            StatusCode = StatusCodes.Status400BadRequest;
            Value = $"Topic '{topic}' already exists.";
        }
    }
}