﻿// Copyright 2023 Sergey Savelev
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace SergeSavel.KafkaRestProxy.Consumer.Responses;

public class PartitionOffsets
{
    public string Topic { get; init; }

    public int Partition { get; init; }

    public long Low { get; init; }

    public long High { get; init; }

    public Offset Current { get; init; }

    public class Offset
    {
        public long Value { get; init; }

        public bool IsSpecial { get; init; }

        public string SpecialValue { get; init; }
    }
}