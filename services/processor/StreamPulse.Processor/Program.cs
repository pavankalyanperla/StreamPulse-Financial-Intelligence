using StreamPulse.Processor;
using StreamPulse.Processor.Application.Interfaces;
using StreamPulse.Processor.Application.Services;
using StreamPulse.Processor.Application.Settings;
using StreamPulse.Processor.Infrastructure.Database;
using StreamPulse.Processor.Infrastructure.Kafka;

var builder = Host.CreateApplicationBuilder(args);

var dbSettings = builder.Configuration
    .GetSection("DatabaseSettings")
    .Get<DatabaseSettings>() ?? new DatabaseSettings();

builder.Services.AddSingleton(dbSettings);
builder.Services.AddSingleton<ICandleAggregator, CandleAggregator>();
builder.Services.AddSingleton<IKafkaProducerService, KafkaProducerService>();
builder.Services.AddSingleton<ICandleRepository, CandleRepository>();
builder.Services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();
builder.Services.AddSingleton<DatabaseInitializer>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();

await host.Services
    .GetRequiredService<DatabaseInitializer>()
    .InitializeAsync(CancellationToken.None);

host.Run();
