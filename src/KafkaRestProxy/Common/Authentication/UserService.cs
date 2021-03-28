using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SergeSavel.KafkaRestProxy.Common.Authentication
{
    public class UserService
    {
        private readonly IDictionary<string, string> _users;


        public UserService(ProxyConfig proxyConfig)
        {
            _users = new Dictionary<string, string>(proxyConfig.Users, StringComparer.OrdinalIgnoreCase);
        }

        public bool IsValidUser(string realm, string username, string password)
        {
            return false;
        }

        public Task<string> AuthenticateAsync(string username, string password)
        {
            return Task.FromResult(Authenticate(username, password));
        }

        public string Authenticate(string username, string password)
        {
            if (_users == null || _users.Count == 0) return "";

            if (!_users.TryGetValue(username, out var storedPassword))
                return null;

            var authenticated = string.IsNullOrEmpty(password)
                ? string.IsNullOrEmpty(storedPassword)
                : password.Equals(storedPassword);
            return authenticated ? username : null;
        }
    }
}