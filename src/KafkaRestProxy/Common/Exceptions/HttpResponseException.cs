﻿// Copyright 2021 Sergey Savelev
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

namespace SergeSavel.KafkaRestProxy.Common.Exceptions
{
    public class HttpResponseException : Exception
    {
        public HttpResponseException(string message, Exception innerException) : base(message, innerException)
        {
            Value = message;
        }

        public HttpResponseException(string message) : base(message)
        {
            Value = message;
        }

        public HttpResponseException(Exception innerException) : base(null, innerException)
        {
        }

        public HttpResponseException()
        {
        }

        public int StatusCode { get; init; } = 500;

        public object Value { get; init; }
    }
}