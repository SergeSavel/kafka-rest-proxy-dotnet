using System.Collections.Generic;

namespace SergeSavel.KafkaRestProxy.Proxy.Configuration
{
    public class BasicAuthUsers : Dictionary<string, BasicAuthUsers.UserInfo>
    {
        public const string SectionName = "BasicAuthUsers";

        public class UserInfo
        {
            public string Password { get; init; }
        }
    }
}