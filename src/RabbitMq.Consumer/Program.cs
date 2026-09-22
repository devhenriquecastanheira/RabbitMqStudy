using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMq.Contracts;

const string exchangeName = "orders.exchange";
const string routingKey = "order.created";
const string queueName = "orders.created";

const string deadLetterExchangeName = "orders.dlx";
const string deadLetterQueueName = "orders.created.dlq";
const string deadLetterRoutingKey = "order.created.dead";

const string retryExchangeName = "orders.retry.exchange";
const string retryQueueName = "orders.created.retry";
const string retryRoutingKey = "order.created.retry";

const int retryDelayMilliseconds = 5000;
const int maxRetries = 3;

var username = Environment.GetEnvironmentVariable("RABBITMQ_DEFAULT_USER");
var password = Environment.GetEnvironmentVariable("RABBITMQ_DEFAULT_PASS");

if (string.IsNullOrWhiteSpace(username) ||
    string.IsNullOrWhiteSpace(password))
{
    Console.WriteLine("Credenciais do RabbitMQ não configuradas.");
    return;
}

var factory = new ConnectionFactory
{
    HostName = "localhost",
    UserName = username,
    Password = password,
    ClientProvidedName = "RabbitMqStudy.Consumer"
};

await using var connection = await factory.CreateConnectionAsync();

await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(
    exchange: exchangeName,
    type: ExchangeType.Direct,
    durable: true
);

await channel.ExchangeDeclareAsync(
    exchange: deadLetterExchangeName,
    type: ExchangeType.Direct,
    durable: true
);

await channel.ExchangeDeclareAsync(
    exchange: retryExchangeName,
    type: ExchangeType.Direct,
    durable: true
);

var retryQueueArguments = new Dictionary<string, object?>
{
    ["x-message-ttl"] = retryDelayMilliseconds,
    ["x-dead-letter-exchange"] = exchangeName,
    ["x-dead-letter-routing-key"] = routingKey
};

await channel.QueueDeclareAsync(
    queue: retryQueueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: retryQueueArguments
);

await channel.QueueBindAsync(
    queue: retryQueueName,
    exchange: retryExchangeName,
    routingKey: retryRoutingKey
);

await channel.QueueDeclareAsync(
    queue: deadLetterQueueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

await channel.QueueBindAsync(
    queue: deadLetterQueueName,
    exchange: deadLetterExchangeName,
    routingKey: deadLetterRoutingKey
);

var queueArguments = new Dictionary<string, object?>
{
    ["x-dead-letter-exchange"] = deadLetterExchangeName,
    ["x-dead-letter-routing-key"] = deadLetterRoutingKey
};

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: queueArguments
);

await channel.QueueBindAsync(
    queue: queueName,
    exchange: exchangeName,
    routingKey: routingKey
);

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (_, ea) =>
{
    try
    {
        var body = ea.Body.ToArray();

        var message = Encoding.UTF8.GetString(body);

        var order = JsonSerializer.Deserialize<OrderCreatedMessage>(message);

        if (order is null)
        {
            throw new Exception("Não foi possível desserializar a mensagem.");
        }

        Console.WriteLine("Pedido recebido:");
        Console.WriteLine($"Id: {order.Id}");
        Console.WriteLine($"Cliente: {order.Customer}");
        Console.WriteLine($"Total: {order.Total}");
        Console.WriteLine($"Criado em: {order.CreatedAt}");

        await channel.BasicAckAsync(
            deliveryTag: ea.DeliveryTag,
            multiple: false
        );

        Console.WriteLine("ACK enviado.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Erro ao processar mensagem: {ex.Message}");

        var retryCount = 0;

        if (ea.BasicProperties.Headers is not null &&
            ea.BasicProperties.Headers.TryGetValue("x-retry-count", out var value))
        {
            retryCount = Convert.ToInt32(value);
        }

        if (retryCount < maxRetries)
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

            await channel.BasicPublishAsync(
                exchange: retryExchangeName,
                routingKey: retryRoutingKey,
                mandatory: false,
                basicProperties: retryProperties,
                body: ea.Body
            );

            await channel.BasicAckAsync(
                deliveryTag: ea.DeliveryTag,
                multiple: false
            );

            Console.WriteLine(
                $"Retry {retryCount + 1}/{maxRetries} agendado."
            );
        }
        else
        {
            await channel.BasicNackAsync(
                deliveryTag: ea.DeliveryTag,
                multiple: false,
                requeue: false
            );

            Console.WriteLine(
                "Limite de retries atingido. Mensagem enviada para a DLQ."
            );
        }
    }
};

await channel.BasicConsumeAsync(
    queue: queueName,
    autoAck: false,
    consumer: consumer
);

Console.WriteLine("Consumer iniciado.");
Console.WriteLine("Aguardando mensagens...");
Console.WriteLine("Pressione ENTER para encerrar.");

Console.ReadLine();