using System.Text;
using System.Text.Json;
using Benchmark.Contracts;
using RabbitMQ.Client;

namespace Benchmark.Brokers;

public class RabbitMqProducer : IProducer
{
    private readonly IConnection _connection;
    private readonly IChannel _channel;
    private readonly string _queueName;

    public RabbitMqProducer(string queueName)
    {
        _queueName = queueName;

        var factory = new ConnectionFactory
        {
            HostName = BrokerConfig.RabbitMqHost,
            UserName = "guest",
            Password = "guest"
        };

        _connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();

        // Habilita publisher confirms: o publish passa a aguardar o ack do
        // broker, igualando a medição com Kafka (ProduceAsync) e Pulsar (Send).
        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);

        _channel = _connection.CreateChannelAsync(options).GetAwaiter().GetResult();
    }

    public async Task CreateQueueAsync()
    {
        await _channel.QueueDeclareAsync(
            queue: _queueName,
            durable: false,
            exclusive: false,
            autoDelete: false
        );
    }

    public async Task PublishAsync(Message message)
    {
        var json = JsonSerializer.Serialize(message);
        var body = Encoding.UTF8.GetBytes(json);

        await _channel.BasicPublishAsync(
            exchange: "",
            routingKey: _queueName,
            mandatory: true,
            basicProperties: new BasicProperties(),
            body: body
        );
    }

    public void Dispose()
    {
        _channel.Dispose();
        _connection.Dispose();
    }
}
