using Microsoft.AspNetCore.SignalR;
using Prometheus;
using StreamPulse.Gateway.Application.Interfaces;

namespace StreamPulse.Gateway.Hubs;

public class StockHub : Hub<IStockHub>
{
    private readonly ILogger<StockHub> _logger;

    private static readonly Gauge _connectedClients = Metrics.CreateGauge(
        "streampulse_signalr_connected_clients",
        "Connected SignalR clients");

    public StockHub(ILogger<StockHub> logger)
    {
        _logger = logger;
    }

    public async Task JoinSymbolGroup(string symbol)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, symbol.ToUpperInvariant());
        _logger.LogInformation("[HUB] {ConnectionId} joined group {Symbol}", Context.ConnectionId, symbol.ToUpperInvariant());
    }

    public async Task JoinAllSymbols()
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, "ALL_SYMBOLS");
        _logger.LogInformation("[HUB] {ConnectionId} joined ALL_SYMBOLS group", Context.ConnectionId);
    }

    public override async Task OnConnectedAsync()
    {
        _connectedClients.Inc();
        _logger.LogInformation("[HUB] Client connected: {ConnectionId}", Context.ConnectionId);
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _connectedClients.Dec();
        _logger.LogInformation("[HUB] Client disconnected: {ConnectionId}", Context.ConnectionId);
        await base.OnDisconnectedAsync(exception);
    }
}
