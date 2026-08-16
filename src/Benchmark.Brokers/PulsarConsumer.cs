using System.Text.Json;
using Benchmark.Contracts;
using DotPulsar;
using DotPulsar.Extensions;

namespace Benchmark.Brokers;

public class PulsarConsumer : IConsumer
{
    private readonly DotPulsar.Abstractions.IPulsarClient _client;
    private readonly DotPulsar.Abstractions.IConsumer<string> _consumer;

    public PulsarConsumer(string topic, string subscription)
    {
        _client = PulsarClient.Builder()
            .ServiceUrl(new Uri(BrokerConfig.PulsarServiceUrl))
            .Build();

        _consumer = _client.NewConsumer(Schema.String)
            .Topic(topic)
            .SubscriptionName(subscription)
            .SubscriptionType(SubscriptionType.Shared)
            .InitialPosition(SubscriptionInitialPosition.Earliest)
            .Create();
    }

    public async Task StartAsync(
        Action<Message> onMessageReceived,
        CancellationToken cancellationToken)
    {
        await foreach (var pulsarMessage in _consumer.Messages(cancellationToken))
        {
            var parsed = JsonSerializer.Deserialize<Message>(
                pulsarMessage.Value()
            );

            onMessageReceived(parsed!);

            await _consumer.Acknowledge(pulsarMessage);
        }
    }

    public void Dispose()
    {
        _consumer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
