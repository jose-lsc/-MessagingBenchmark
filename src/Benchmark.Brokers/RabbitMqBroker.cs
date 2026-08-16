using Benchmark.Contracts;

namespace Benchmark.Brokers;

public class RabbitMqBroker : IBenchmarkBroker
{
    private readonly string _queueName;

    public RabbitMqBroker(string queueName)
    {
        _queueName = queueName;
    }

    public string Name => "RabbitMQ";

    public async Task PrepareAsync(int partitions)
    {
        using var producer = new RabbitMqProducer(_queueName);

        await producer.CreateQueueAsync();
    }

    public IProducer CreateProducer()
    {
        return new RabbitMqProducer(_queueName);
    }

    public IConsumer CreateConsumer()
    {
        return new RabbitMqConsumer(_queueName);
    }
}
