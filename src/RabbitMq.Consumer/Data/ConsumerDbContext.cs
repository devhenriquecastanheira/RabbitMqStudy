using Microsoft.EntityFrameworkCore;

namespace RabbitMq.Consumer.Data;

public class ConsumerDbContext : DbContext
{
    public DbSet<ProcessedMessage> ProcessedMessages { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlite("Data Source=consumer.db");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ProcessedMessage>()
            .HasKey(x => x.MessageId);
    }
}