using StreamPulse.AlertService;
using StreamPulse.AlertService.Application.Interfaces;
using StreamPulse.AlertService.Application.Settings;
using StreamPulse.AlertService.Infrastructure.Database;
using StreamPulse.AlertService.Infrastructure.Email;
using StreamPulse.AlertService.Infrastructure.Kafka;

var builder = WebApplication.CreateBuilder(args);

// Settings
var kafkaSettings = builder.Configuration.GetSection("KafkaSettings").Get<KafkaSettings>() ?? new KafkaSettings();
var dbSettings = builder.Configuration.GetSection("DatabaseSettings").Get<DatabaseSettings>() ?? new DatabaseSettings();
var emailSettings = builder.Configuration.GetSection("EmailSettings").Get<EmailSettings>() ?? new EmailSettings();

builder.Services.AddSingleton(kafkaSettings);
builder.Services.AddSingleton(dbSettings);
builder.Services.AddSingleton(emailSettings);

// Infrastructure
builder.Services.AddSingleton<DatabaseInitializer>();
builder.Services.AddSingleton<IAlertRepository, AlertRepository>();
builder.Services.AddSingleton<IEmailNotifier, EmailNotifier>();
builder.Services.AddSingleton<IKafkaAlertConsumer, KafkaAlertConsumer>();

// Worker
builder.Services.AddHostedService<Worker>();

var app = builder.Build();

// DB init on startup
await app.Services.GetRequiredService<DatabaseInitializer>().InitializeAsync(CancellationToken.None);

// Minimal API endpoints
app.MapGet("/health", () => Results.Ok(new { status = "ok", service = "alert-service" }));

app.MapGet("/alerts", async (
    string? symbol,
    string? severity,
    int? limit,
    IAlertRepository repo,
    CancellationToken ct) =>
{
    var take = limit.HasValue && limit.Value > 0 ? limit.Value : 20;
    var alerts = await repo.GetAlertsAsync(symbol, severity, take, ct);
    return Results.Ok(alerts);
});

app.MapGet("/alerts/stats", async (IAlertRepository repo, CancellationToken ct) =>
{
    var stats = await repo.GetStatsAsync(ct);
    return Results.Ok(stats);
});

app.MapGet("/alerts/high", async (IAlertRepository repo, CancellationToken ct) =>
{
    var alerts = await repo.GetAlertsAsync(null, "HIGH", 10, ct);
    return Results.Ok(alerts);
});

app.Run();
