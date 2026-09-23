using RabbitMQ.Client;
using RabbitMq.Consumer.Configuration;

namespace RabbitMq.Consumer.Messaging;

public static class RabbitMqTopology
{
    public static async Task ConfigureAsync(IChannel channel)
    {
        await channel.ExchangeDeclareAsync(
            exchange: RabbitMqSettings.ExchangeName,
            type: ExchangeType.Direct,
            durable: true
        );

        await channel.ExchangeDeclareAsync(
            exchange: RabbitMqSettings.DeadLetterExchangeName,
            type: ExchangeType.Direct,
            durable: true
        );

        await channel.ExchangeDeclareAsync(
            exchange: RabbitMqSettings.RetryExchangeName,
            type: ExchangeType.Direct,
            durable: true
        );

        var retryQueueArguments = new Dictionary<string, object?>
        {
            ["x-message-ttl"] = RabbitMqSettings.RetryDelayMilliseconds,
            ["x-dead-letter-exchange"] = RabbitMqSettings.ExchangeName,
            ["x-dead-letter-routing-key"] = RabbitMqSettings.RoutingKey
        };

        await channel.QueueDeclareAsync(
            queue: RabbitMqSettings.RetryQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: retryQueueArguments
        );

        await channel.QueueBindAsync(
            queue: RabbitMqSettings.RetryQueueName,
            exchange: RabbitMqSettings.RetryExchangeName,
            routingKey: RabbitMqSettings.RetryRoutingKey
        );

        await channel.QueueDeclareAsync(
            queue: RabbitMqSettings.DeadLetterQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null
        );

        await channel.QueueBindAsync(
            queue: RabbitMqSettings.DeadLetterQueueName,
            exchange: RabbitMqSettings.DeadLetterExchangeName,
            routingKey: RabbitMqSettings.DeadLetterRoutingKey
        );

        var queueArguments = new Dictionary<string, object?>
        {
            ["x-dead-letter-exchange"] = RabbitMqSettings.DeadLetterExchangeName,
            ["x-dead-letter-routing-key"] = RabbitMqSettings.DeadLetterRoutingKey
        };

        await channel.QueueDeclareAsync(
            queue: RabbitMqSettings.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArguments
        );

        await channel.QueueBindAsync(
            queue: RabbitMqSettings.QueueName,
            exchange: RabbitMqSettings.ExchangeName,
            routingKey: RabbitMqSettings.RoutingKey
        );
    }
}