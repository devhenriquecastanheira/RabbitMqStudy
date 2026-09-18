using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMq.Contracts;

const string exchangeName = "orders.exchange";
const string queueName = "orders.created";
const string routingKey = "order.created";

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
    ClientProvidedName = "RabbitMqStudy.Producer"
};

await using var connection = await factory.CreateConnectionAsync();

await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(
    exchange: exchangeName,
    type: ExchangeType.Direct,
    durable: true
);

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

await channel.QueueBindAsync(
    queue: queueName,
    exchange: exchangeName,
    routingKey: routingKey
);

var order = new OrderCreatedMessage(
    Guid.NewGuid(),
    "Henrique",
    199.90m,
    DateTime.UtcNow
);

var json = JsonSerializer.Serialize(order);

var body = Encoding.UTF8.GetBytes(json);

var properties = new BasicProperties
{
    ContentType = "application/json",
    DeliveryMode = DeliveryModes.Persistent
};

await channel.BasicPublishAsync(
    exchange: exchangeName,
    routingKey: routingKey,
    mandatory: false,
    basicProperties: properties,
    body: body
);

Console.WriteLine("Mensagem enviada com sucesso!");
Console.WriteLine(json);