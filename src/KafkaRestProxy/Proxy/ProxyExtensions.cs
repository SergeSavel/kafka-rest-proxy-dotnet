using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SergeSavel.KafkaRestProxy.Proxy.Authentication;
using SergeSavel.KafkaRestProxy.Proxy.Configuration;

namespace SergeSavel.KafkaRestProxy.Proxy
{
    public static class ProxyExtensions
    {
        public static void AddProxy(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddOptions<BasicAuthUsers>()
                .Bind(configuration.GetSection(BasicAuthUsers.SectionName))
                .ValidateDataAnnotations();

            services.AddAuthentication("Basic")
                .AddScheme<AuthenticationSchemeOptions, BasicAuthHandler>("Basic", null);

            services.AddSingleton<UserService>();
        }
    }
}