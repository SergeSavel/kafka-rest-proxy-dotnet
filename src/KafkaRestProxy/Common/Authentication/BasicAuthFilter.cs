﻿using System;
using System.Net.Http.Headers;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Server.HttpSys;
using Microsoft.Extensions.DependencyInjection;

namespace SergeSavel.KafkaRestProxy.Common.Authentication
{
    public class BasicAuthFilter : IAuthorizationFilter
    {
        private static readonly string Scheme = AuthenticationSchemes.Basic.ToString();
        private readonly string _realm;

        public BasicAuthFilter(string realm)
        {
            _realm = realm;
            if (string.IsNullOrWhiteSpace(_realm))
                throw new ArgumentNullException(nameof(realm), @"Please provide a non-empty realm value.");
        }

        public void OnAuthorization(AuthorizationFilterContext context)
        {
            try
            {
                string authHeader = context.HttpContext.Request.Headers["Authorization"];
                if (authHeader != null)
                {
                    var authHeaderValue = AuthenticationHeaderValue.Parse(authHeader);
                    if (authHeaderValue.Scheme.Equals(Scheme, StringComparison.OrdinalIgnoreCase))
                    {
                        var credentials = Encoding.UTF8
                            .GetString(Convert.FromBase64String(authHeaderValue.Parameter ?? string.Empty))
                            .Split(':', 2);

                        if (credentials.Length == 2 && IsAuthorized(context, credentials[0], credentials[1]))
                            return;
                    }
                }

                ReturnUnauthorizedResult(context);
            }
            catch (FormatException)
            {
                ReturnUnauthorizedResult(context);
            }
        }

        public bool IsAuthorized(AuthorizationFilterContext context, string username, string password)
        {
            var authService = context.HttpContext.RequestServices.GetRequiredService<BasicAuthService>();
            return authService.IsValidUser(_realm, username, password);
        }

        private void ReturnUnauthorizedResult(AuthorizationFilterContext context)
        {
            // Return 401 and a basic authentication challenge (causes browser to show login dialog)
            context.HttpContext.Response.Headers["WWW-Authenticate"] = $"Basic realm=\"{_realm}\"";
            context.Result = new UnauthorizedResult();
        }
    }
}