using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SergeSavel.KafkaRestProxy.Common.Authentication
{
    public class UserService
    {
        private readonly IDictionary<string, UserWithPassword> _users;

        public UserService(ProxyConfig proxyConfig)
        {
            _users = new Dictionary<string, UserWithPassword>(StringComparer.OrdinalIgnoreCase);

            foreach (var (username, password) in proxyConfig.Users)
                _users.Add(username, new UserWithPassword
                {
                    Name = username,
                    Password = password
                });
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