namespace Benchmark.Contracts;

public interface IBenchmarkBroker
{
    string Name { get; }

    Task PrepareAsync(int partitions);

    IProducer CreateProducer();

    IConsumer CreateConsumer();
}
