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
using System.ComponentModel.DataAnnotations;

namespace SergeSavel.KafkaRestProxy.AdminClient.Requests
{
    public class CreateAdminClientRequest
    {
        [Required] public string Name { get; init; }

        [Required] public IReadOnlyDictionary<string, string> Config { get; init; }

        [Required] [Range(1000, 86400000)] public int ExpirationTimeoutMs { get; init; }
    }
}