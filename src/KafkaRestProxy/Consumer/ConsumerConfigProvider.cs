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

using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using SergeSavel.KafkaRestProxy.Common;

namespace SergeSavel.KafkaRestProxy.Consumer
{
    public static class ConsumerConfigProvider
    {
        public static ConsumerConfig GetConfig(IConfiguration configuration = null)
        {
            var clientConfig = ClientConfigProvider.GetConfig(configuration);

            var result = new ConsumerConfig(clientConfig)
            {
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = "kafka-rest-proxy-dotnet"
            };

            //result.Debug = "all";

            configuration?.Bind("Kafka:Consumer", result);

            return result;
        }
    }
}