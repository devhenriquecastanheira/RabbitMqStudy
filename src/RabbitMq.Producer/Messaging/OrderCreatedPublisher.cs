using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMq.Contracts;
using RabbitMq.Producer.Configuration;

namespace RabbitMq.Producer.Messaging;

public class OrderCreatedPublisher
{
    private readonly IChannel _channel;

    public OrderCreatedPublisher(IChannel channel)
    {
        _channel = channel;
    }

    public async Task PublishAsync(OrderCreatedMessage order)
    {
        var json = JsonSerializer.Serialize(order);

        var body = Encoding.UTF8.GetBytes(json);

        var properties = new BasicProperties
        {
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent
        };

        await _channel.BasicPublishAsync(
            exchange: RabbitMqSettings.ExchangeName,
            routingKey: RabbitMqSettings.RoutingKey,
            mandatory: true,
            basicProperties: properties,
            body: body
        );

        Console.WriteLine("Mensagem confirmada pelo RabbitMQ!");
        Console.WriteLine(json);
    }
}