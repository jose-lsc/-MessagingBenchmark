using System.Text.Json;
using Benchmark.Contracts;
using DotPulsar;
using DotPulsar.Extensions;

namespace Benchmark.Brokers;

public class PulsarProducer : IProducer
{
    private readonly DotPulsar.Abstractions.IPulsarClient _client;
    private readonly DotPulsar.Abstractions.IProducer<string> _producer;

    public PulsarProducer(string topic)
    {
        _client = PulsarClient.Builder()
            .ServiceUrl(new Uri(BrokerConfig.PulsarServiceUrl))
            .Build();

        _producer = _client.NewProducer(Schema.String)
            .Topic(topic)
            .Create();
    }

    public async Task PublishAsync(Message message)
    {
        var json = JsonSerializer.Serialize(message);

        // Send aguarda o ack do broker (entrega confirmada).
        await _producer.Send(json);
    }

    public void Dispose()
    {
        _producer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
