using RabbitMQ.Client;
using RabbitMq.Consumer.Data;
using RabbitMq.Consumer.Messaging;

var username =
    Environment.GetEnvironmentVariable("RABBITMQ_DEFAULT_USER");

var password =
    Environment.GetEnvironmentVariable("RABBITMQ_DEFAULT_PASS");

if (string.IsNullOrWhiteSpace(username) ||
    string.IsNullOrWhiteSpace(password))
{
    Console.WriteLine(
        "Credenciais do RabbitMQ não configuradas."
    );

    return;
}

await using (var dbContext = new ConsumerDbContext())
{
    await dbContext.Database.EnsureCreatedAsync();
}

var factory = new ConnectionFactory
{
    HostName = "localhost",
    UserName = username,
    Password = password,
    ClientProvidedName = "RabbitMqStudy.Consumer"
};

await using var connection =
    await factory.CreateConnectionAsync();

await using var channel =
    await connection.CreateChannelAsync();

await channel.BasicQosAsync(
    prefetchSize: 0,
    prefetchCount: 1,
    global: false
);

await RabbitMqTopology.ConfigureAsync(channel);

var consumer = new OrderCreatedConsumer(channel);

await consumer.StartAsync();

Console.WriteLine("Consumer iniciado.");
Console.WriteLine("Aguardando mensagens...");
Console.WriteLine("Pressione ENTER para encerrar.");

Console.ReadLine();