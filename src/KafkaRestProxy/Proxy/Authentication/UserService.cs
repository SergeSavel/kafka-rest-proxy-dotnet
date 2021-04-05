using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using SergeSavel.KafkaRestProxy.Proxy.Configuration;

namespace SergeSavel.KafkaRestProxy.Proxy.Authentication
{
    public class UserService
    {
        private readonly IDictionary<string, UserWithPassword> _basicAuthUsers;

        public UserService(IOptions<BasicAuthUsers> basicAuthUsersOptions)
        {
            if (basicAuthUsersOptions.Value != null)
            {
                _basicAuthUsers = new Dictionary<string, UserWithPassword>(StringComparer.OrdinalIgnoreCase);

                foreach (var (userName, userInfo) in basicAuthUsersOptions.Value)
                    _basicAuthUsers.Add(userName, new UserWithPassword
                    {
                        Name = userName,
                        Password = userInfo.Password
                    });
            }
        }

        public static User DefaultUser { get; } = new() {Name = string.Empty};

        public Task<User> AuthenticateAsync(string scheme, string username, string password)
        {
            return Task.FromResult(Authenticate(scheme, username, password));
        }

        public User Authenticate(string scheme, string username, string password)
        {
            if (_basicAuthUsers == null || _basicAuthUsers.Count == 0) return DefaultUser;

            if (username == null) return null;

            if (!_basicAuthUsers.TryGetValue(username, out var user))
                return null;

            var authenticated = string.IsNullOrEmpty(password)
                ? string.IsNullOrEmpty(user.Password)
                : password.Equals(user.Password);
            return authenticated ? MapUser(user) : null;
        }

        private static User MapUser(UserWithPassword source)
        {
            return new()
            {
                Name = source.Name
            };
        }

        private class UserWithPassword
        {
            public string Name { get; init; }
            public string Password { get; init; }
        }
    }
}