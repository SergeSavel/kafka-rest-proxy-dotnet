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

using System;
using System.Reflection;
using System.Runtime.Versioning;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace SergeSavel.KafkaRestProxy.Proxy
{
    [ApiController]
    [AllowAnonymous]
    [Route("")]
    public class RootController : ControllerBase
    {
        [HttpGet]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public IActionResult GetVersion()
        {
            var assembly = Assembly.GetExecutingAssembly();
            var result = new
            {
                Assembly = new
                {
                    assembly.GetCustomAttribute<AssemblyTitleAttribute>()?.Title,
                    assembly.GetCustomAttribute<AssemblyProductAttribute>()?.Product,
                    assembly.GetCustomAttribute<AssemblyCompanyAttribute>()?.Company,
                    assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration,
                    assembly.GetCustomAttribute<AssemblyFileVersionAttribute>()?.Version,
                    TargetFramework = assembly.GetCustomAttribute<TargetFrameworkAttribute>()?.FrameworkName
                },
                Runtime = new
                {
                    Version = Environment.Version.ToString(),
                    Is64Bit = Environment.Is64BitProcess
                }
            };
            return Ok(result);
        }
    }
}