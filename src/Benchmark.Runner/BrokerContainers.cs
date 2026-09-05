namespace Benchmark.Runner;

internal static class BrokerContainers
{
    public const string RabbitMq = "rabbitmq";
    public const string Kafka = "kafka";
    public const string Pulsar = "pulsar";
   
    public static string FromKey(string brokerKey) => brokerKey.ToLowerInvariant() switch
    {
        "rabbitmq" => RabbitMq,
        "kafka" => Kafka,
        "pulsar" => Pulsar,
        _ => throw new ArgumentException($"Broker desconhecido: {brokerKey}", nameof(brokerKey))
    };
}
