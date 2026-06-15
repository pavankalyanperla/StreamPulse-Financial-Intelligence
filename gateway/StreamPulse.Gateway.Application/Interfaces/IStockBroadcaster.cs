using StreamPulse.Gateway.Application.Models;

namespace StreamPulse.Gateway.Application.Interfaces;

public interface IStockBroadcaster
{
    Task BroadcastTickAsync(LiveTick tick, CancellationToken ct = default);
    Task BroadcastCandleAsync(OhlcvCandle candle, CancellationToken ct = default);
    Task BroadcastForecastAsync(PriceForecast forecast, CancellationToken ct = default);
    Task BroadcastAlertAsync(AnomalyAlert alert, CancellationToken ct = default);
}
