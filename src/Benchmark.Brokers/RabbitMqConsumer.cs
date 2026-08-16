using System.Text;
using System.Text.Json;
using Benchmark.Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Benchmark.Brokers;

public class RabbitMqConsumer : IConsumer
{
    private readonly IConnection _connection;
    private readonly IChannel _channel;
    private readonly string _queueName;

    public RabbitMqConsumer(string queueName)
    {
        _queueName = queueName;

        var factory = new ConnectionFactory
        {
            HostName = BrokerConfig.RabbitMqHost,
            UserName = "guest",
            Password = "guest"
        };

        _connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
        _channel = _connection.CreateChannelAsync().GetAwaiter().GetResult();
    }

    public async Task StartAsync(
        Action<Message> onMessageReceived,
        CancellationToken cancellationToken)
    {
        await _channel.QueueDeclareAsync(
            queue: _queueName,
            durable: false,
            exclusive: false,
            autoDelete: false
        );

        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.ReceivedAsync += async (sender, args) =>
        {
            var json = Encoding.UTF8.GetString(args.Body.ToArray());

            var message = JsonSerializer.Deserialize<Message>(json);

            onMessageReceived(message!);

            await Task.CompletedTask;
        };

        await _channel.BasicConsumeAsync(
            queue: _queueName,
            autoAck: true,
            consumer: consumer
        );
    }

    public void Dispose()
    {
        _channel.Dispose();
        _connection.Dispose();
    }
}
