using StreamPulse.Processor.Application.Models;

namespace StreamPulse.Processor.Application.Interfaces;

public interface ICandleAggregator
{
    void AddTick(StockTick tick);
    IEnumerable<OhlcvCandle> FlushCompletedCandles(DateTimeOffset currentTime);
}
