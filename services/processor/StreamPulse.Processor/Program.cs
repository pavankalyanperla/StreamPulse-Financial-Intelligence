using Prometheus;
using StreamPulse.Processor;
using StreamPulse.Processor.Application.Interfaces;
using StreamPulse.Processor.Application.Services;
using StreamPulse.Processor.Application.Settings;
using StreamPulse.Processor.Infrastructure.Database;
using StreamPulse.Processor.Infrastructure.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.UseUrls("http://0.0.0.0:5100");

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

var app = builder.Build();

app.UseHttpMetrics();

await app.Services
    .GetRequiredService<DatabaseInitializer>()
    .InitializeAsync(CancellationToken.None);

app.MapMetrics();

app.MapGet("/candles/{symbol}", async (string symbol, int limit, ICandleRepository repo, CancellationToken ct) =>
{
    var candles = await repo.GetRecentCandlesAsync(symbol.ToUpper(), limit <= 0 ? 60 : limit, ct);
    return Results.Ok(candles);
});

await app.RunAsync();
