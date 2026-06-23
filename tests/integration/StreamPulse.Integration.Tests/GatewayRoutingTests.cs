using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.SignalR.Client;

namespace StreamPulse.Integration.Tests;

[TestFixture]
public class GatewayRoutingTests
{
    private const string GatewayBase = "http://localhost:5000";

    // shared HttpClient — one per test run, not per test
    private static readonly HttpClient Http = new() { Timeout = TimeSpan.FromSeconds(15) };

    // ── /health ───────────────────────────────────────────────────────────────

    [Test, CancelAfter(15_000)]
    public async Task Gateway_Health_Returns_Ok()
    {
        var response = await Http.GetAsync($"{GatewayBase}/health");

        response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK,
            "gateway /health must return 200 OK");

        string body = await response.Content.ReadAsStringAsync();
        body.Should().Contain("ok", "health response must include status:ok");

        TestContext.Progress.WriteLine($"[GW] /health → {(int)response.StatusCode}  {body.Trim()}");
    }

    // ── /api/alerts ───────────────────────────────────────────────────────────

    [Test, CancelAfter(15_000)]
    public async Task Gateway_Alerts_Route_Works()
    {
        var response = await Http.GetAsync($"{GatewayBase}/api/alerts?limit=5");

        response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK,
            "Ocelot must proxy /api/alerts to the alert service successfully");

        string body = await response.Content.ReadAsStringAsync();
        var root = JsonDocument.Parse(body).RootElement;
        root.ValueKind.Should().Be(JsonValueKind.Array,
            "/api/alerts must return a JSON array");

        TestContext.Progress.WriteLine($"[GW] /api/alerts → {root.GetArrayLength()} alerts");
    }

    // ── /api/sentiment/{symbol} ───────────────────────────────────────────────

    [Test, CancelAfter(15_000)]
    public async Task Gateway_Sentiment_Route_Works()
    {
        var response = await Http.GetAsync($"{GatewayBase}/api/sentiment/AAPL");

        response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK,
            "Ocelot must proxy /api/sentiment/AAPL to the sentiment service");

        string body = await response.Content.ReadAsStringAsync();
        body.Should().Contain("symbol",
            "sentiment response must contain a 'symbol' field");

        TestContext.Progress.WriteLine($"[GW] /api/sentiment/AAPL → {body[..Math.Min(120, body.Length)]}");
    }

    // ── /api/forecasts/{symbol} ───────────────────────────────────────────────

    [Test, CancelAfter(15_000)]
    public async Task Gateway_Forecast_Route_Works()
    {
        // Ocelot maps /api/forecasts/{everything} → /forecast/{everything} on downstream
        var response = await Http.GetAsync($"{GatewayBase}/api/forecasts/AAPL");

        response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK,
            "Ocelot must proxy /api/forecasts/AAPL to the forecasting service");

        string body = await response.Content.ReadAsStringAsync();
        // Python forecasting service uses snake_case (predicted_close)
        body.Should().Contain("predicted_close",
            "forecast response must contain a 'predicted_close' field");

        TestContext.Progress.WriteLine($"[GW] /api/forecasts/AAPL → {body[..Math.Min(120, body.Length)]}");
    }

    // ── SignalR hub ───────────────────────────────────────────────────────────

    [Test, CancelAfter(30_000)]
    public async Task SignalR_Hub_Connects_And_Receives_Ticks()
    {
        var receivedTicks = new List<(string Symbol, decimal Price)>();
        var firstTick     = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var connection = new HubConnectionBuilder()
            .WithUrl($"{GatewayBase}/hubs/stock")
            .Build();

        // a) Register ReceiveTick handler — gateway broadcasts LiveTick as snake_case JSON
        connection.On<JsonElement>("ReceiveTick", payload =>
        {
            string  symbol = payload.GetProperty("symbol").GetString() ?? "";
            decimal price  = payload.GetProperty("price").GetDecimal();
            receivedTicks.Add((symbol, price));
            firstTick.TrySetResult(true);
        });

        // b) Connect
        await connection.StartAsync();
        connection.State.Should().Be(HubConnectionState.Connected,
            "SignalR must connect to /hubs/stock");
        TestContext.Progress.WriteLine($"[GW] SignalR connected (ConnectionId={connection.ConnectionId})");

        // c) Join the broadcast group for all symbols
        await connection.InvokeAsync("JoinAllSymbols");
        TestContext.Progress.WriteLine("[GW] Joined ALL_SYMBOLS group");

        // d) Wait up to 15 s for the first broadcasted tick
        await Task.WhenAny(firstTick.Task, Task.Delay(15_000));

        // e) Assertions
        receivedTicks.Should().HaveCountGreaterThanOrEqualTo(1,
            "gateway must broadcast at least 1 tick via SignalR within 15 s of joining ALL_SYMBOLS");

        var (sym, price) = receivedTicks[0];
        price.Should().BeGreaterThan(0, "received tick price must be positive");
        TestContext.Progress.WriteLine($"[GW] First tick received: {sym} @ ${price:F4}");

        // f) Clean shutdown
        await connection.StopAsync();
        await connection.DisposeAsync();
    }
}
