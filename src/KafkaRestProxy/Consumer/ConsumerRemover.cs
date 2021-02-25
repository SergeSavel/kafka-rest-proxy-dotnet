using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace pro.savel.KafkaRestProxy.Consumer
{
    public class ConsumerRemover : BackgroundService
    {
        private const long PeriodMs = 10000;

        private readonly ConsumerService _consumerService;

        public ConsumerRemover(ConsumerService consumerService)
        {
            _consumerService = consumerService;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var stopwatch = Stopwatch.StartNew();

                _consumerService.ConsumerProvider.RemoveExpiredConsumers();

                var remainingMs = PeriodMs - stopwatch.ElapsedMilliseconds;
                if (remainingMs > 0)
                    await Task.Delay((int) remainingMs, stoppingToken);
            }
        }
    }
}