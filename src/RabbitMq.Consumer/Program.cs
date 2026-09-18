using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMq.Contracts;

const string queueName = "orders.created";

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

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (_, ea) =>
{
    var body = ea.Body.ToArray();

    var message = Encoding.UTF8.GetString(body);
    
    var order = JsonSerializer.Deserialize<OrderCreatedMessage>(message);

    if (order is not null)
    {
        Console.WriteLine("Pedido recebido:");
        Console.WriteLine($"Id: {order.Id}");
        Console.WriteLine($"Cliente: {order.Customer}");
        Console.WriteLine($"Total: {order.Total}");
        Console.WriteLine($"Criado em: {order.CreatedAt}");
    }

    await channel.BasicAckAsync(
        deliveryTag: ea.DeliveryTag,
        multiple: false
    );
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