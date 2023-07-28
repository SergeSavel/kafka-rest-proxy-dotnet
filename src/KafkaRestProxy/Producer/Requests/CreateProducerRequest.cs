// Copyright 2023 Sergey Savelev
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

using System.ComponentModel.DataAnnotations;
using SergeSavel.KafkaRestProxy.Common.Contract;

namespace SergeSavel.KafkaRestProxy.Producer.Requests;

public class CreateProducerRequest
{
    [Required] public string Name { get; init; }
    [Required] public Dictionary<string, string> Config { get; init; }
    [Required] [Range(1000, 86400000)] public int ExpirationTimeoutMs { get; init; }
    public KeyValueType KeyType { get; init; } = KeyValueTypeDefaults.Key;
    public KeyValueType ValueType { get; init; } = KeyValueTypeDefaults.Value;
}