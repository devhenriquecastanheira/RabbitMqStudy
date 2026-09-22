using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMq.Contracts;

const string exchangeName = "orders.exchange";
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

var channelOptions = new CreateChannelOptions(
    publisherConfirmationsEnabled: true,
    publisherConfirmationTrackingEnabled: true
);

await using var channel =
    await connection.CreateChannelAsync(channelOptions);

await channel.ExchangeDeclareAsync(
    exchange: exchangeName,
    type: ExchangeType.Direct,
    durable: true
);

var order = new OrderCreatedMessage(
    Guid.NewGuid(),
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

try
{
    await channel.BasicPublishAsync(
        exchange: exchangeName,
        routingKey: routingKey,
        mandatory: true,
        basicProperties: properties,
        body: body
    );

    Console.WriteLine("Mensagem confirmada pelo RabbitMQ!");
    Console.WriteLine(json);
}
catch (Exception ex)
{
    Console.WriteLine($"Erro ao publicar mensagem: {ex.Message}");
}