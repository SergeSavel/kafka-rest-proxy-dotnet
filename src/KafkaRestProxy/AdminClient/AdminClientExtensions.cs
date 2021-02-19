using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace pro.savel.KafkaRestProxy.AdminClient
{
    public static class AdminClientExtensions
    {
        public static void AddAdminClient(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton(AdminClientConfigProvider.GetConfig(configuration));
            services.AddScoped<AdminClientService>();
        }
    }
}