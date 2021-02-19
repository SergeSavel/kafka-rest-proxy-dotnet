using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace pro.savel.KafkaRestProxy.Producer
{
    public static class ProducerExtensions
    {
        public static void AddProducer(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton(ProducerConfigProvider.GetConfig(configuration));
            services.AddSingleton<ProducerService>();
        }
    }
}