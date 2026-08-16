namespace Benchmark.Contracts;

public interface IProducer : IDisposable
{
    Task PublishAsync(Message message);
}
