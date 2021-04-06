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

using System;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.HttpSys;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SergeSavel.KafkaRestProxy.Proxy.Authentication
{
    public class BasicAuthHandler : AuthenticationHandler<AuthenticationSchemeOptions>
    {
        private const string Realm = "default";
        private static readonly string BasicScheme = AuthenticationSchemes.Basic.ToString();
        private readonly UserService _userService;

        private string _failureMessage;

        public BasicAuthHandler(IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger,
            UrlEncoder encoder, ISystemClock clock, UserService userService) : base(options, logger, encoder, clock)
        {
            _userService = userService;
        }

        protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            // skip authentication if endpoint has [AllowAnonymous] attribute
            var endpoint = Context.GetEndpoint();
            if (endpoint?.Metadata.GetMetadata<IAllowAnonymous>() != null)
                return AuthenticateResult.NoResult();

            User user = null;

            if (Request.Headers.TryGetValue("Authorization", out var authHeader))
            {
                try
                {
                    var authHeaderValue = AuthenticationHeaderValue.Parse(authHeader);
                    if (BasicScheme.Equals(authHeaderValue.Scheme, StringComparison.OrdinalIgnoreCase))
                    {
                        var credentialBytes = Convert.FromBase64String(authHeaderValue.Parameter ?? string.Empty);
                        var credentials = Encoding.UTF8.GetString(credentialBytes).Split(':', 2);
                        user = await _userService.AuthenticateAsync(BasicScheme, credentials[0], credentials[1]);
                    }
                }
                catch
                {
                    _failureMessage = "Invalid Authorization Header";
                    return AuthenticateResult.Fail(_failureMessage);
                }

                if (user == null)
                {
                    _failureMessage = "Invalid Username or Password";
                    return AuthenticateResult.Fail(_failureMessage);
                }
            }
            else
            {
                user = await _userService.AuthenticateAsync(null, null, null);
                if (user == null)
                {
                    _failureMessage = "Missing Authorization Header";
                    return AuthenticateResult.Fail(_failureMessage);
                }
            }

            var claims = new[]
            {
                new Claim(ClaimTypes.Name, user.Name)
            };
            var identity = new ClaimsIdentity(claims, Scheme.Name);
            var principal = new ClaimsPrincipal(identity);
            var ticket = new AuthenticationTicket(principal, Scheme.Name);

            return AuthenticateResult.Success(ticket);
        }

        protected override async Task HandleChallengeAsync(AuthenticationProperties properties)
        {
            Response.Headers["WWW-Authenticate"] = $"Basic realm=\"{Realm}\"";
            Response.StatusCode = 401;
            await Response.WriteAsync(_failureMessage);
        }
    }
}