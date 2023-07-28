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

namespace SergeSavel.KafkaRestProxy.AdminClient.Contract;

public class AclBinding
{
    /// <summary>The resource pattern.</summary>
    public ResourcePattern Pattern { get; set; } = null;

    /// <summary>The access control entry.</summary>
    public AccessControlEntry Entry { get; set; } = null;

    /// <summary>
    ///     Create a filter which matches only this AclBinding.
    /// </summary>
    public AclBindingFilter ToFilter()
    {
        return new AclBindingFilter
        {
            PatternFilter = Pattern.ToFilter(),
            EntryFilter = Entry.ToFilter()
        };
    }
}