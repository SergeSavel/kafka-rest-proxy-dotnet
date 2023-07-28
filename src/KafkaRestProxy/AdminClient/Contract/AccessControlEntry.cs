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

public class AccessControlEntry
{
    /// <summary>
    ///     The principal this access control entry refers to.
    /// </summary>
    public string Principal { get; set; } = null;

    /// <summary>
    ///     The host where the call is allowed to come from. Or `*` for all hosts.
    /// </summary>
    public string Host { get; set; } = null;

    /// <summary>The operation/s specified by this entry.</summary>
    public AclOperation Operation { get; set; } = AclOperation.Unknown;

    /// <summary>The permission type for the specified operation.</summary>
    public AclPermissionType Permission { get; set; } = AclPermissionType.Unknown;

    /// <summary>
    ///     Create a filter which matches only this AccessControlEntry.
    /// </summary>
    public AccessControlEntryFilter ToFilter()
    {
        return new AccessControlEntryFilter
        {
            Principal = Principal,
            Host = Host,
            Operation = Operation,
            Permission = Permission
        };
    }
}