using Benchmark.Contracts;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Benchmark.Brokers;

public class KafkaBroker : IBenchmarkBroker
{
    private readonly string _topic;
    private readonly string _groupId;

    public KafkaBroker(string topic)
    {
        _topic = topic;
        _groupId = "benchmark-" + Guid.NewGuid().ToString("N")[..8];
    }

    public string Name => "Kafka";

    public async Task PrepareAsync(int partitions)
    {
        using var admin = new AdminClientBuilder(
            new AdminClientConfig
            {
                BootstrapServers = BrokerConfig.KafkaBootstrapServers
            }
        ).Build();

        await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = _topic,
                NumPartitions = Math.Max(1, partitions),
                ReplicationFactor = 1
            }
        ]);

        // Dá um respiro para o KRaft propagar os metadados de liderança das partições
        // antes dos múltiplos producers começarem a disparar mensagens em paralelo.
        await Task.Delay(5500);
    }

    public IProducer CreateProducer()
    {
        return new KafkaProducer(_topic);
    }

    public IConsumer CreateConsumer()
    {
        return new KafkaConsumer(_topic, _groupId);
    }
}