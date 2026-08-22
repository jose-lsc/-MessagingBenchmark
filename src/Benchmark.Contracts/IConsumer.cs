namespace Benchmark.Contracts;

public interface IConsumer : IDisposable
{
    Task StartAsync(
        Action<Message> onMessageReceived,
        CancellationToken cancellationToken
    );
}
