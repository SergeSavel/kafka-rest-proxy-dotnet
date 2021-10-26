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

using System.Collections.Generic;
using SergeSavel.KafkaRestProxy.Common.Contract;

namespace SergeSavel.KafkaRestProxy.AdminClient.Responses
{
    public class TopicMetadata
    {
        public string Topic { get; init; }

        public ICollection<PartitionMetadata> Partitions { get; init; }

        public Error Error { get; init; }

        public int? OriginatingBrokerId { get; init; }

        public string OriginatingBrokerName { get; init; }

        public class PartitionMetadata
        {
            public int Partition { get; init; }

            public int? Leader { get; init; }

            public ICollection<int> Replicas { get; init; }

            public ICollection<int> InSyncReplicas { get; init; }

            public Error Error { get; init; }
        }
    }
}