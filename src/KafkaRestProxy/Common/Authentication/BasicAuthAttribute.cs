using System;
using Microsoft.AspNetCore.Mvc;

namespace SergeSavel.KafkaRestProxy.Common.Authentication
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Method)]
    public class BasicAuthAttribute : TypeFilterAttribute
    {
        public BasicAuthAttribute(string realm = @"default") : base(typeof(BasicAuthFilter))
        {
            Arguments = new object[] {realm};
        }
    }
}