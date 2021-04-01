using System;
using System.Reflection;
using System.Runtime.Versioning;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace SergeSavel.KafkaRestProxy.Proxy
{
    [ApiController]
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
                Environment = new
                {
                    Version = Environment.Version.ToString(),
                    Is64Bit = Environment.Is64BitProcess
                }
            };
            return Ok(result);
        }
    }
}