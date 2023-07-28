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

public enum AclOperation
{
    /// <summary>Unknown</summary>
    Unknown,

    /// <summary>In a filter, matches any AclOperation</summary>
    Any,

    /// <summary>ALL the operations</summary>
    All,

    /// <summary>READ operation</summary>
    Read,

    /// <summary>WRITE operation</summary>
    Write,

    /// <summary>CREATE operation</summary>
    Create,

    /// <summary>DELETE operation</summary>
    Delete,

    /// <summary>ALTER operation</summary>
    Alter,

    /// <summary>DESCRIBE operation</summary>
    Describe,

    /// <summary>CLUSTER_ACTION operation</summary>
    ClusterAction,

    /// <summary>DESCRIBE_CONFIGS operation</summary>
    DescribeConfigs,

    /// <summary>ALTER_CONFIGS operation</summary>
    AlterConfigs,

    /// <summary>IDEMPOTENT_WRITE operation</summary>
    IdempotentWrite
}