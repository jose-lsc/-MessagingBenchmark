using Benchmark.Contracts;

namespace Benchmark.Brokers;

public class PulsarBroker : IBenchmarkBroker
{
    private const string TopicPrefix = "persistent://public/default/";

    private readonly string _topic;
    private readonly string _subscription;

    public PulsarBroker(string topic)
    {
        _topic = TopicPrefix + topic;
        _subscription = "benchmark-" + Guid.NewGuid().ToString("N")[..8];
    }

    public string Name => "Pulsar";

    public Task PrepareAsync(int partitions)
    {
        // O tópico do Pulsar é criado automaticamente no primeiro uso.
        return Task.CompletedTask;
    }

    public IProducer CreateProducer()
    {
        return new PulsarProducer(_topic);
    }

    public IConsumer CreateConsumer()
    {
        return new PulsarConsumer(_topic, _subscription);
    }
}
