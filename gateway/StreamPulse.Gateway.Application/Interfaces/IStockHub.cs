using StreamPulse.Gateway.Application.Models;

namespace StreamPulse.Gateway.Application.Interfaces;

public interface IStockHub
{
    Task ReceiveTick(LiveTick tick);
    Task ReceiveCandle(OhlcvCandle candle);
    Task ReceiveForecast(PriceForecast forecast);
    Task ReceiveAlert(AnomalyAlert alert);
}
