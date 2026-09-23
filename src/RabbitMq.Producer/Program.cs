using RabbitMQ.Client;
using RabbitMq.Contracts;
using RabbitMq.Producer.Configuration;
using RabbitMq.Producer.Messaging;

var username =
    Environment.GetEnvironmentVariable("RABBITMQ_DEFAULT_USER");

var password =
    Environment.GetEnvironmentVariable("RABBITMQ_DEFAULT_PASS");

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

await using var connection =
    await factory.CreateConnectionAsync();

var channelOptions = new CreateChannelOptions(
    publisherConfirmationsEnabled: true,
    publisherConfirmationTrackingEnabled: true
);

await using var channel =
    await connection.CreateChannelAsync(channelOptions);

await channel.ExchangeDeclareAsync(
    exchange: RabbitMqSettings.ExchangeName,
    type: ExchangeType.Direct,
    durable: true
);

var publisher = new OrderCreatedPublisher(channel);

var order = new OrderCreatedMessage(
    Guid.NewGuid(),
    Guid.NewGuid(),
    "Henrique",
    199.90m,
    DateTime.UtcNow
);

try
{
    await publisher.PublishAsync(order);
}
catch (Exception ex)
{
    Console.WriteLine($"Erro ao publicar mensagem: {ex.Message}");
}