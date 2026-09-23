using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMq.Consumer.Configuration;
using RabbitMq.Consumer.Data;
using RabbitMq.Contracts;

namespace RabbitMq.Consumer.Messaging;

public class OrderCreatedConsumer
{
    private readonly IChannel _channel;

    public OrderCreatedConsumer(IChannel channel)
    {
        _channel = channel;
    }

    public async Task StartAsync()
    {
        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.ReceivedAsync += HandleMessageAsync;

        await _channel.BasicConsumeAsync(
            queue: RabbitMqSettings.QueueName,
            autoAck: false,
            consumer: consumer
        );
    }

    private async Task HandleMessageAsync(
        object sender,
        BasicDeliverEventArgs ea)
    {
        try
        {
            var body = ea.Body.ToArray();

            var message = Encoding.UTF8.GetString(body);

            var order =
                JsonSerializer.Deserialize<OrderCreatedMessage>(message);

            if (order is null)
            {
                throw new Exception(
                    "Não foi possível desserializar a mensagem."
                );
            }

            await using var dbContext = new ConsumerDbContext();

            var alreadyProcessed =
                await dbContext.ProcessedMessages.AnyAsync(
                    x => x.MessageId == order.MessageId
                );

            if (alreadyProcessed)
            {
                Console.WriteLine(
                    $"Mensagem {order.MessageId} já foi processada. " +
                    "Ignorando duplicata."
                );

                await _channel.BasicAckAsync(
                    deliveryTag: ea.DeliveryTag,
                    multiple: false
                );

                return;
            }

            Console.WriteLine("Pedido recebido:");
            Console.WriteLine($"MessageId: {order.MessageId}");
            Console.WriteLine($"Id: {order.Id}");
            Console.WriteLine($"Cliente: {order.Customer}");
            Console.WriteLine($"Total: {order.Total}");
            Console.WriteLine($"Criado em: {order.CreatedAt}");

            dbContext.ProcessedMessages.Add(
                new ProcessedMessage
                {
                    MessageId = order.MessageId,
                    ProcessedAt = DateTime.UtcNow
                }
            );

            await dbContext.SaveChangesAsync();

            await _channel.BasicAckAsync(
                deliveryTag: ea.DeliveryTag,
                multiple: false
            );

            Console.WriteLine("ACK enviado.");
        }
        catch (Exception ex)
        {
            await HandleFailureAsync(ea, ex);
        }
    }

    private async Task HandleFailureAsync(
        BasicDeliverEventArgs ea,
        Exception exception)
    {
        Console.WriteLine(
            $"Erro ao processar mensagem: {exception.Message}"
        );

        var retryCount = GetRetryCount(ea);

        if (retryCount < RabbitMqSettings.MaxRetries)
        {
            var retryProperties = new BasicProperties
            {
                ContentType = "application/json",
                DeliveryMode = DeliveryModes.Persistent,
                Headers = new Dictionary<string, object?>
                {
                    ["x-retry-count"] = retryCount + 1
                }
            };

            await _channel.BasicPublishAsync(
                exchange: RabbitMqSettings.RetryExchangeName,
                routingKey: RabbitMqSettings.RetryRoutingKey,
                mandatory: false,
                basicProperties: retryProperties,
                body: ea.Body
            );

            await _channel.BasicAckAsync(
                deliveryTag: ea.DeliveryTag,
                multiple: false
            );

            Console.WriteLine(
                $"Retry {retryCount + 1}/" +
                $"{RabbitMqSettings.MaxRetries} agendado."
            );

            return;
        }

        await _channel.BasicNackAsync(
            deliveryTag: ea.DeliveryTag,
            multiple: false,
            requeue: false
        );

        Console.WriteLine(
            "Limite de retries atingido. " +
            "Mensagem enviada para a DLQ."
        );
    }

    private static int GetRetryCount(BasicDeliverEventArgs ea)
    {
        if (ea.BasicProperties.Headers is null)
            return 0;

        if (!ea.BasicProperties.Headers.TryGetValue(
                "x-retry-count",
                out var value))
        {
            return 0;
        }

        return Convert.ToInt32(value);
    }
}