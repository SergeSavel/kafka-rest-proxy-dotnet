using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;

namespace SergeSavel.KafkaRestProxy
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args)
        {
            return Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder
                        .UseStartup<Startup>()
                        .UseUrls("http://localhost:8086");
                })
                .UseWindowsService();
        }
    }
}