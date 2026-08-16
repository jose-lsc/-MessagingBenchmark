using System.Text.Json;
using Benchmark.Contracts;
using Confluent.Kafka;

namespace Benchmark.Brokers;

public class KafkaConsumer : IConsumer
{
    private readonly IConsumer<Null, string> _consumer;
    private readonly string _topic;

    public KafkaConsumer(string topic, string groupId)
    {
        _topic = topic;

        _consumer = new ConsumerBuilder<Null, string>(
            new ConsumerConfig
            {
                BootstrapServers = BrokerConfig.KafkaBootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = true
            }).Build();
    }

    public async Task StartAsync(
        Action<Message> onMessageReceived,
        CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_topic);

        // Confluent.Kafka não tem consume assíncrono nativo; o Consume()
        // é síncrono. Rodamos o loop em uma Task em background para que o
        // StartAsync devolva o controle ao chamador (ConsumerRunner) em vez
        // de bloquear a thread de quem o invocou.
        await Task.Run(
            () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = _consumer.Consume(TimeSpan.FromMilliseconds(100));

                    if (result is null || result.IsPartitionEOF)
                    {
                        continue;
                    }

                    var message = JsonSerializer.Deserialize<Message>(
                        result.Message.Value
                    );

                    onMessageReceived(message!);
                }
            },
            cancellationToken);
    }

    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
    }
}
