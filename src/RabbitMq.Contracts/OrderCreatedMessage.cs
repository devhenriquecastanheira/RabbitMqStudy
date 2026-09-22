namespace RabbitMq.Contracts;

public record OrderCreatedMessage(
    Guid MessageId,
    Guid Id,
    string Customer,
    decimal Total,
    DateTime CreatedAt
);