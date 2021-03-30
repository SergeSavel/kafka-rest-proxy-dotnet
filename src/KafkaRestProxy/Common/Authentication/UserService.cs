using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;

namespace SergeSavel.KafkaRestProxy.Common.Authentication
{
    public class UserService
    {
        private readonly IDictionary<string, UserWithPassword> _users;

        public UserService(IOptions<ProxyConfig> proxyConfigOptions)
        {
            _users = new Dictionary<string, UserWithPassword>(StringComparer.OrdinalIgnoreCase);

            foreach (var user in proxyConfigOptions.Value.Users)
            {
                if (user.Name == null) throw new ApplicationException("Username must be present.");

                _users.Add(user.Name, new UserWithPassword
                {
                    Name = user.Name,
                    Password = user.Password
                });
            }
        }

        public static User DefaultUser { get; } = new() {Name = string.Empty};

        public Task<User> AuthenticateAsync(string username, string password)
        {
            return Task.FromResult(Authenticate(username, password));
        }

        public User Authenticate(string username, string password)
        {
            if (_users == null || _users.Count == 0) return DefaultUser;

            if (!_users.TryGetValue(username, out var user))
                return null;

            var authenticated = string.IsNullOrEmpty(password)
                ? string.IsNullOrEmpty(user.Password)
                : password.Equals(user.Password);
            return authenticated ? CopyUser(user) : null;
        }

        private static User CopyUser(User source)
        {
            return new()
            {
                Name = source.Name
            };
        }

        private class UserWithPassword : User
        {
            public string Password { get; init; }
        }
    }
}