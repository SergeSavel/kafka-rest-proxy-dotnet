using System;
using System.Collections.Generic;

namespace SergeSavel.KafkaRestProxy.Common.Authentication
{
    public class BasicAuthService
    {
        private readonly IDictionary<string, string> _users;


        public BasicAuthService(ProxyConfig proxyConfig)
        {
            _users = new Dictionary<string, string>(proxyConfig.Users, StringComparer.OrdinalIgnoreCase);
        }

        public bool IsValidUser(string realm, string username, string password)
        {
            if (_users == null || _users.Count == 0) return true;

            if (_users.TryGetValue(username, out var storedPassword))
                return string.IsNullOrWhiteSpace(password)
                    ? string.IsNullOrWhiteSpace(storedPassword)
                    : password.Equals(storedPassword);

            return false;
        }
    }
}