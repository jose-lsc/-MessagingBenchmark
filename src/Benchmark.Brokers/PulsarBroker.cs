using Benchmark.Contracts;
using DotPulsar;
using System.Net.Http.Json;

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

    public async Task PrepareAsync(int partitions)
    {
        if (partitions <= 1)
        {
            return;
        }

        // Usa a REST API do Pulsar para criar tópico particionado
        var httpClient = new HttpClient();
        var adminUrl = BrokerConfig.PulsarServiceUrl.Replace("pulsar://", "http://").Replace(":6650", ":8080");
        
        var topicName = _topic.Replace("persistent://public/default/", "");
        
        var url = $"{adminUrl}/admin/v2/persistent/public/default/{topicName}/partitions";

        var response = await httpClient.PutAsync(url, JsonContent.Create(partitions));
       
        if (!response.IsSuccessStatusCode)
        {
            var error = await response.Content.ReadAsStringAsync();
            throw new InvalidOperationException($"Failed to create partitioned topic: {error}");
        }
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
