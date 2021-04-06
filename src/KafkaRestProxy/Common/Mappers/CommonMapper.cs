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

using Confluent.Kafka;
using Error = SergeSavel.KafkaRestProxy.Common.Contract.Error;

namespace SergeSavel.KafkaRestProxy.Common.Mappers
{
    public static class CommonMapper
    {
        public static Error Map(Confluent.Kafka.Error source)
        {
            if (source.Code == ErrorCode.NoError)
                return null;

            return new Error
            {
                Code = (int) source.Code,
                Reason = source.Reason,
                IsBrokerError = source.IsBrokerError,
                IsLocalError = source.IsLocalError,
                IsFatal = source.IsFatal
            };
        }
    }
}