using StreamPulse.Processor.Application.Interfaces;
using StreamPulse.Processor.Application.Models;

namespace StreamPulse.Processor.Application.Services;

public sealed class CandleAggregator : ICandleAggregator
{
    // symbol -> (minuteTimestamp -> candle)
    private readonly Dictionary<string, Dictionary<DateTimeOffset, OhlcvCandle>> _candles = new();

    public void AddTick(StockTick tick)
    {
        var minute = new DateTimeOffset(
            tick.Timestamp.Year, tick.Timestamp.Month, tick.Timestamp.Day,
            tick.Timestamp.Hour, tick.Timestamp.Minute, 0, 0,
            tick.Timestamp.Offset);

        if (!_candles.TryGetValue(tick.Symbol, out var symbolCandles))
        {
            symbolCandles = new Dictionary<DateTimeOffset, OhlcvCandle>();
            _candles[tick.Symbol] = symbolCandles;
        }

        if (!symbolCandles.TryGetValue(minute, out var candle))
        {
            candle = new OhlcvCandle
            {
                Symbol = tick.Symbol,
                OpenPrice = tick.Price,
                HighPrice = tick.Price,
                LowPrice = tick.Price,
                ClosePrice = tick.Price,
                TotalVolume = tick.Volume,
                CandleTime = minute,
                TickCount = 1,
            };
            symbolCandles[minute] = candle;
            return;
        }

        if (tick.Price > candle.HighPrice) candle.HighPrice = tick.Price;
        if (tick.Price < candle.LowPrice) candle.LowPrice = tick.Price;
        candle.ClosePrice = tick.Price;
        candle.TotalVolume += tick.Volume;
        candle.TickCount++;
    }

    public IEnumerable<OhlcvCandle> FlushCompletedCandles(DateTimeOffset currentTime)
    {
        var currentMinute = new DateTimeOffset(
            currentTime.Year, currentTime.Month, currentTime.Day,
            currentTime.Hour, currentTime.Minute, 0, 0,
            currentTime.Offset);

        var completed = new List<OhlcvCandle>();

        foreach (var (symbol, symbolCandles) in _candles)
        {
            var done = symbolCandles.Keys
                .Where(k => k < currentMinute)
                .ToList();

            foreach (var key in done)
            {
                completed.Add(symbolCandles[key]);
                symbolCandles.Remove(key);
            }
        }

        return completed;
    }
}
