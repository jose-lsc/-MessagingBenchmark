using System.Diagnostics;
using Benchmark.Contracts;

namespace Benchmark.Runner;

public static class ProducerRunner
{
    public static async Task RunAsync(Scenario scenario, IBenchmarkBroker broker)
    {
        int baseCount = scenario.MessageCount / scenario.Producers;
        int remainder = scenario.MessageCount % scenario.Producers;

        var tasks = new List<Task>();

        for (int i = 1; i <= scenario.Producers; i++)
        {
            int producerId = i;

            int messagesPerProducer =
                baseCount + (producerId <= remainder ? 1 : 0);

            tasks.Add(
                Task.Run(async () =>
                {
                    using var producer = broker.CreateProducer();

                    await PublishWithRateAsync(
                        producer,
                        scenario,
                        producerId,
                        messagesPerProducer);
                })
            );
        }

        await Task.WhenAll(tasks);
    }

    private static async Task PublishWithRateAsync(
        IProducer producer,
        Scenario scenario,
        int producerId,
        int count)
    {
        var stopwatch = Stopwatch.StartNew();

        for (int i = 0; i < count; i++)
        {
            // Controle de taxa por lotes: Task.Delay tem granularidade
            // de ~15ms, então não dá para esperar a cada mensagem.
            if (scenario.MessagesPerSecond > 0)
            {
                int batchSize = Math.Max(1, scenario.MessagesPerSecond / 100);

                if (i > 0 && i % batchSize == 0)
                {
                    double expectedTimeMs =
                        (i * 1000.0) / scenario.MessagesPerSecond;

                    double remainingMs =
                        expectedTimeMs - stopwatch.Elapsed.TotalMilliseconds;

                    if (remainingMs > 0)
                    {
                        await Task.Delay((int)remainingMs);
                    }
                }
            }

            var message = new Message
            {
                Id = (producerId * 1_000_000L) + i,
                Timestamp = DateTime.UtcNow,
                Payload = new string('A', scenario.MessageSizeBytes)
            };

            await producer.PublishAsync(message);
        }

        stopwatch.Stop();
    }
}
