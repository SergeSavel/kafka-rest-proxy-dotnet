using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace SergeSavel.KafkaRestProxy.Consumer
{
    public static class ConsumerExtensions
    {
        public static void AddConsumer(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton(ConsumerConfigProvider.GetConfig(configuration));
            services.AddSingleton<ConsumerService>();
            services.AddHostedService<ConsumerRemover>();
        }
    }
}