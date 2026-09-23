namespace RabbitMq.Consumer.Configuration;

public static class RabbitMqSettings
{
    public const string ExchangeName = "orders.exchange";
    public const string RoutingKey = "order.created";
    public const string QueueName = "orders.created";

    public const string DeadLetterExchangeName = "orders.dlx";
    public const string DeadLetterQueueName = "orders.created.dlq";
    public const string DeadLetterRoutingKey = "order.created.dead";

    public const string RetryExchangeName = "orders.retry.exchange";
    public const string RetryQueueName = "orders.created.retry";
    public const string RetryRoutingKey = "order.created.retry";

    public const int RetryDelayMilliseconds = 5000;
    public const int MaxRetries = 3;
}