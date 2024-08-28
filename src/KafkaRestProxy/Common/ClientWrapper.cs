// Copyright 2023 Sergey Savelev
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace SergeSavel.KafkaRestProxy.Common;

public abstract class ClientWrapper : IDisposable
{
    private static readonly Random XRandom = new();
    private readonly TimeSpan _expirationTimeout;

    protected ClientWrapper(string name, IDictionary<string, string> config, TimeSpan expirationTimeout)
    {
        Name = name;
        User = GetUsernameFromConfig(config);
        _expirationTimeout = expirationTimeout;
        UpdateExpiration();
    }

    public Guid Id { get; } = Guid.NewGuid();
    public string Name { get; }
    public string Owner { get; init; }
    public DateTime ExpiresAt { get; private set; }
    public bool IsExpired => DateTime.Now >= ExpiresAt;
    public string User { get; }
    public string Token { get; } = GenerateToken();

    public abstract void Dispose();

    public void UpdateExpiration()
    {
        ExpiresAt = DateTime.Now + _expirationTimeout;
    }

    private static string GetUsernameFromConfig(IDictionary<string, string> config)
    {
        if (!config.TryGetValue("sasl.username", out var username)) return null;
        return username;
    }

    private static string GenerateToken()
    {
        const int length = 8;
        var chars = new char[length];
        for (var i = 0; i < length; i++) chars[i] = (char)XRandom.Next('A', 'A' + 26);

        return new string(chars);
    }
}