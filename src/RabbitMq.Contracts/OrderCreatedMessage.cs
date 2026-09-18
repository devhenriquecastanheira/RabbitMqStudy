namespace RabbitMq.Contracts;

public record OrderCreatedMessage(
    Guid Id,
    string Customer,
    decimal Total,
    DateTime CreatedAt
);