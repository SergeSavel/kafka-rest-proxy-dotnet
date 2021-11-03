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

using System.Linq;
using Confluent.Kafka;
using BrokerMetadata = SergeSavel.KafkaRestProxy.Common.Responses.BrokerMetadata;
using Error = SergeSavel.KafkaRestProxy.Common.Contract.Error;
using TopicMetadata = SergeSavel.KafkaRestProxy.Common.Responses.TopicMetadata;

namespace SergeSavel.KafkaRestProxy.Common.Mappers
{
    public static class CommonMapper
    {
        public static BrokerMetadata Map(Confluent.Kafka.BrokerMetadata source)
        {
            return new BrokerMetadata
            {
                Id = source.BrokerId,
                Host = source.Host,
                Port = source.Port
            };
        }

        public static TopicMetadata Map(Confluent.Kafka.TopicMetadata source)
        {
            return new TopicMetadata
            {
                Topic = source.Topic,
                Partitions = source.Partitions.Select(Map).ToArray(),
                //Error = Map(source.Error)
            };
        }

        public static TopicMetadata.PartitionMetadata Map(PartitionMetadata source)
        {
            return new TopicMetadata.PartitionMetadata
            {
                Partition = source.PartitionId,
                Leader = source.Leader,
                Replicas = source.Replicas,
                InSyncReplicas = source.InSyncReplicas
                //Error = Map(source.Error)
            };
        }

        public static Error Map(Confluent.Kafka.Error source)
        {
            if (source.Code == ErrorCode.NoError)
                return null;

            return new Error
            {
                Code = (int)source.Code,
                Reason = source.Reason,
                IsBrokerError = source.IsBrokerError,
                IsLocalError = source.IsLocalError,
                IsFatal = source.IsFatal
            };
        }
    }
}