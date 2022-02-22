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

using Microsoft.Extensions.Options;
using SergeSavel.KafkaRestProxy.Proxy.Configuration;

namespace SergeSavel.KafkaRestProxy.Proxy.Authentication;

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

    public static User DefaultUser { get; } = new() { Name = string.Empty };

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
        return new User
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