// Copyright 2023 Sergey Savelev
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

namespace SergeSavel.KafkaRestProxy.AdminClient.Contract;

public class ResourcePattern
{
    /// <summary>The resource type.</summary>
    public ResourceType ResourceType { get; set; } = ResourceType.Unknown;

    /// <summary>
    ///     The resource name, which depends on the resource type.
    ///     For ResourceBroker the resource name is the broker id.
    /// </summary>
    public string Name { get; set; } = null;

    /// <summary>
    ///     The resource pattern, which controls how the pattern will match resource names.
    /// </summary>
    public ResourcePatternType PatternType { get; set; } = ResourcePatternType.Unknown;

    /// <summary>
    ///     Create a filter which matches only this ResourcePattern.
    /// </summary>
    public ResourcePatternFilter ToFilter()
    {
        return new ResourcePatternFilter
        {
            ResourceType = ResourceType,
            Name = Name,
            PatternType = PatternType
        };
    }
}