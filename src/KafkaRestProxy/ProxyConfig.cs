using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace SergeSavel.KafkaRestProxy
{
    public class ProxyConfig
    {
        public const string SectionName = "Proxy";

        public bool IndentOutput { get; init; } = true;

        public ICollection<User> Users { get; init; }

        public class User
        {
            [Required] public string Name { get; init; }
            public string Password { get; init; }
        }
    }
}