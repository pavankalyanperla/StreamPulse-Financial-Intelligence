using Microsoft.AspNetCore.SignalR;
using Prometheus;
using StreamPulse.Gateway.Application.Interfaces;
using StreamPulse.Gateway.Application.Models;
using StreamPulse.Gateway.Hubs;

namespace StreamPulse.Gateway.Services;

public class StockBroadcaster : IStockBroadcaster
{
    private readonly IHubContext<StockHub, IStockHub> _hub;
    private readonly ILogger<StockBroadcaster> _logger;

    private static readonly Counter _ticksBroadcast = Metrics.CreateCounter(
        "streampulse_ticks_broadcast_total",
        "Ticks broadcast via SignalR",
        new CounterConfiguration { LabelNames = new[] { "symbol" } });

    public StockBroadcaster(IHubContext<StockHub, IStockHub> hub, ILogger<StockBroadcaster> logger)
    {
        _hub = hub;
        _logger = logger;
    }

    public async Task BroadcastTickAsync(LiveTick tick, CancellationToken ct = default)
    {
        await _hub.Clients.Group(tick.Symbol).ReceiveTick(tick);
        await _hub.Clients.Group("ALL_SYMBOLS").ReceiveTick(tick);
        _ticksBroadcast.WithLabels(tick.Symbol).Inc();
    }

    public async Task BroadcastCandleAsync(OhlcvCandle candle, CancellationToken ct = default)
    {
        await _hub.Clients.Group(candle.Symbol).ReceiveCandle(candle);
        await _hub.Clients.Group("ALL_SYMBOLS").ReceiveCandle(candle);
    }

    public async Task BroadcastForecastAsync(PriceForecast forecast, CancellationToken ct = default)
    {
        await _hub.Clients.Group(forecast.Symbol).ReceiveForecast(forecast);
        await _hub.Clients.Group("ALL_SYMBOLS").ReceiveForecast(forecast);
        _logger.LogInformation("[GATEWAY] Forecast broadcast: {Symbol} predicted={Price}", forecast.Symbol, forecast.PredictedClose);
    }

    public async Task BroadcastAlertAsync(AnomalyAlert alert, CancellationToken ct = default)
    {
        await _hub.Clients.Group(alert.Symbol).ReceiveAlert(alert);
        await _hub.Clients.Group("ALL_SYMBOLS").ReceiveAlert(alert);
        _logger.LogInformation("[GATEWAY] Alert broadcast: {Severity} {AlertType} — {Symbol}", alert.Severity, alert.AlertType, alert.Symbol);
    }
}
