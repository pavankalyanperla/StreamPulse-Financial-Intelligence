using Ocelot.DependencyInjection;
using Ocelot.Middleware;
using Prometheus;
using StackExchange.Redis;
using StreamPulse.Gateway.Application.Interfaces;
using StreamPulse.Gateway.Application.Settings;
using StreamPulse.Gateway.Hubs;
using StreamPulse.Gateway.Infrastructure;
using StreamPulse.Gateway.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration
    .AddJsonFile("appsettings.json", optional: false)
    .AddJsonFile($"appsettings.{builder.Environment.EnvironmentName}.json", optional: true)
    .AddJsonFile("ocelot.json", optional: false)
    .AddEnvironmentVariables();

// Settings
builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("KafkaSettings"));
builder.Services.Configure<RedisSettings>(builder.Configuration.GetSection("RedisSettings"));

// Redis
var redisConnStr = builder.Configuration.GetSection("RedisSettings:ConnectionString").Value ?? "localhost:6380";
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnStr));

// SignalR
builder.Services.AddSignalR();

// Infrastructure
builder.Services.AddSingleton<IRedisTickCache, RedisTickCache>();
builder.Services.AddSingleton<IStockBroadcaster, StockBroadcaster>();
builder.Services.AddSingleton<IKafkaGatewayConsumer, KafkaGatewayConsumer>();
builder.Services.AddSingleton<KafkaTickConsumer>();

// Ocelot
builder.Services.AddOcelot();

// CORS for Angular frontend
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
        policy.WithOrigins("http://localhost:4200")
              .AllowAnyMethod()
              .AllowAnyHeader()
              .AllowCredentials());
});

var app = builder.Build();

app.UseCors();
app.UseHttpMetrics();

// Metrics endpoint — before Ocelot branch so it isn't proxied
app.MapMetrics();

// Direct endpoints handled before Ocelot branch
app.MapHub<StockHub>("/hubs/stocks");

app.MapGet("/health", () => Results.Ok(new { status = "ok", service = "gateway" }));

app.MapGet("/ticks", async (IRedisTickCache cache, CancellationToken ct) =>
{
    var ticks = await cache.GetAllTicksAsync(ct);
    return Results.Ok(ticks);
});

app.MapGet("/ticks/{symbol}", async (string symbol, IRedisTickCache cache, CancellationToken ct) =>
{
    var tick = await cache.GetTickAsync(symbol.ToUpperInvariant(), ct);
    return tick is null ? Results.NotFound() : Results.Ok(tick);
});

// Ocelot handles only /api/* proxy routes — does not interfere with endpoints above
app.MapWhen(
    ctx => ctx.Request.Path.StartsWithSegments("/api"),
    ocelotPipeline => ocelotPipeline.UseOcelot().GetAwaiter().GetResult()
);

// Start Kafka consumers as background tasks after app starts
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
lifetime.ApplicationStarted.Register(() =>
{
    var cts = new CancellationTokenSource();
    lifetime.ApplicationStopping.Register(() => cts.Cancel());

    var gatewayConsumer = app.Services.GetRequiredService<IKafkaGatewayConsumer>();
    var tickConsumer = app.Services.GetRequiredService<KafkaTickConsumer>();

    Task.Run(() => gatewayConsumer.StartConsumingAsync(cts.Token));
    Task.Run(() => tickConsumer.StartConsumingAsync(cts.Token));
});

await app.RunAsync();
