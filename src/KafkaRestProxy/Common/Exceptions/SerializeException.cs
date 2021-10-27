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

namespace SergeSavel.KafkaRestProxy.Common.Exceptions
{
    public class SerializeException : HttpResponseException
    {
        public SerializeException(string message) : base(message)
        {
            StatusCode = 400;
        }

        public SerializeException(string message, Exception innerException) : base(message, innerException)
        {
            StatusCode = 400;
        }
    }
}