using System.Collections.Concurrent;
using Benchmark.Contracts;

namespace Benchmark.Runner;

public static class ConsumerRunner
{
    public static async Task<ConsumerResult> StartConsumers(
        int quantity,
        int expectedMessages,
        IBenchmarkBroker broker)
    {
        int receivedMessages = 0;

        var latenciesMs = new ConcurrentQueue<double>();

        var completion = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        using var cts = new CancellationTokenSource();

        var consumers = new List<IConsumer>();
        var consumerTasks = new List<Task>();

        try
        {
            for (int i = 0; i < quantity; i++)
            {
                var consumer = broker.CreateConsumer();

                consumers.Add(consumer);

                // Não aguardamos aqui: Rabbit retorna logo após se inscrever,
                // Kafka/Pulsar ficam num loop de consumo até o cancelamento.
                consumerTasks.Add(
                    consumer.StartAsync(
                        message =>
                        {
                            double latencyMs =
                                (DateTime.UtcNow - message.Timestamp).TotalMilliseconds;

                            latenciesMs.Enqueue(latencyMs);

                            int received = Interlocked.Increment(
                                ref receivedMessages
                            );

                            if (received >= expectedMessages)
                            {
                                completion.TrySetResult(true);
                            }
                        },
                        cts.Token
                    )
                );
            }

            Console.WriteLine(
                $"{quantity} consumer(s) iniciado(s)."
            );

            await completion.Task;

            cts.Cancel();

            try
            {
                await Task.WhenAll(consumerTasks);
            }
            catch (OperationCanceledException)
            {
                // Cancelamento esperado ao encerrar o consumo.
            }
            catch (Exception)
            {
                // Ignora erros de encerramento dos consumidores.
            }

            Console.WriteLine(
                $"Todas as {expectedMessages} mensagens foram consumidas."
            );
        }
        finally
        {
            foreach (var consumer in consumers)
            {
                consumer.Dispose();
            }
        }

        return new ConsumerResult
        {
            ReceivedCount = receivedMessages,
            LatenciesMs = latenciesMs.ToList()
        };
    }
}
