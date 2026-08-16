using System.Text.Json;
using Benchmark.Contracts;
using Confluent.Kafka;

namespace Benchmark.Brokers;

public class KafkaProducer : IProducer
{
    private readonly IProducer<Null, string> _producer;
    private readonly string _topic;

    public KafkaProducer(string topic)
    {
        _topic = topic;

        _producer = new ProducerBuilder<Null, string>(
            new ProducerConfig
            {
                BootstrapServers = BrokerConfig.KafkaBootstrapServers,
                // O default (5ms) deixa cada mensagem esperando o batch
                // fechar. Como o benchmark aguarda o ack de cada mensagem
                // (como no Rabbit com publisher confirms), isso adiciona
                // ~5ms+ por mensagem e derruba o throughput para ~60 msg/s.
                // Com 0ms o envio é imediato (~1100 msg/s).
                LingerMs = 0
            }).Build();
    }

    public async Task PublishAsync(Message message)
    {
        var json = JsonSerializer.Serialize(message);

        // ProduceAsync aguarda o ack do broker (entrega confirmada).
        await _producer.ProduceAsync(
            _topic,
            new Message<Null, string> { Value = json });
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
