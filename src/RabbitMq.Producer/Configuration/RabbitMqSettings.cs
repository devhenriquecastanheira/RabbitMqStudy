namespace RabbitMq.Producer.Configuration;

public static class RabbitMqSettings
{
    public const string ExchangeName = "orders.exchange";
    public const string RoutingKey = "order.created";
}