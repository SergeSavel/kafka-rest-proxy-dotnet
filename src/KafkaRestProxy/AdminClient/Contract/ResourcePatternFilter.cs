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

namespace SergeSavel.KafkaRestProxy.AdminClient.Contract;

public class ResourcePatternFilter
{
    /// <summary>The resource type this filter matches.</summary>
    public ResourceType ResourceType { get; set; } = ResourceType.Any;

    /// <summary>
    ///     The resource name this filter matches, which depends on the resource type.
    ///     For ResourceBroker the resource name is the broker id.
    /// </summary>
    public string Name { get; set; } = null;

    /// <summary>
    ///     The resource pattern type of this filter.
    ///     If <see cref="F:Confluent.Kafka.Admin.ResourcePatternType.Any" />, the filter will match patterns regardless of
    ///     pattern type.
    ///     If <see cref="F:Confluent.Kafka.Admin.ResourcePatternType.Match" />, the filter will match patterns that would
    ///     match the supplied name,
    ///     including a matching prefixed and wildcards patterns.
    ///     If any other resource pattern type, the filter will match only patterns with the same type.
    /// </summary>
    public ResourcePatternType PatternType { get; set; } = ResourcePatternType.Any;
}