using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace SergeSavel.KafkaRestProxy.Admin
{
    public static class AdminClientExtensions
    {
        public static void AddAdminClient(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton(AdminClientConfigProvider.GetConfig(configuration));
            services.AddSingleton<AdminClientService>(); // ???
        }
    }
}