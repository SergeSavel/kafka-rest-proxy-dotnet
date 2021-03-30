using System;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SergeSavel.KafkaRestProxy.Common.Authentication
{
    public class BasicAuthHandler : AuthenticationHandler<AuthenticationSchemeOptions>
    {
        private const string Realm = "default";
        private readonly UserService _userService;

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

            User user;

            if (Request.Headers.TryGetValue("Authorization", out var authHeader))
            {
                try
                {
                    var authHeaderValue = AuthenticationHeaderValue.Parse(authHeader);
                    var credentialBytes = Convert.FromBase64String(authHeaderValue.Parameter ?? string.Empty);
                    var credentials = Encoding.UTF8.GetString(credentialBytes).Split(':', 2);
                    user = await _userService.AuthenticateAsync(credentials[0], credentials[1]);
                }
                catch
                {
                    return AuthenticateResult.Fail("Invalid Authorization Header");
                }

                if (user == null)
                    return AuthenticateResult.Fail("Invalid Username or Password");
            }
            else
            {
                user = await _userService.AuthenticateAsync(null, null);
                if (user == null)
                    return AuthenticateResult.Fail("Missing Authorization Header");
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

        protected override Task HandleChallengeAsync(AuthenticationProperties properties)
        {
            Response.Headers["WWW-Authenticate"] = $"Basic realm=\"{Realm}\"";
            Response.StatusCode = 401;
            return Task.CompletedTask;
        }
    }
}