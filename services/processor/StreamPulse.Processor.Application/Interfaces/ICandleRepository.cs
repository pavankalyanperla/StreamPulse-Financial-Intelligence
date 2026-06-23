using StreamPulse.Processor.Application.Models;

namespace StreamPulse.Processor.Application.Interfaces;

public interface ICandleRepository
{
    Task SaveCandleAsync(OhlcvCandle candle, CancellationToken cancellationToken);
    Task<IEnumerable<OhlcvCandle>> GetCandlesAsync(string symbol, DateTimeOffset from, DateTimeOffset to, CancellationToken cancellationToken);
    Task<IEnumerable<OhlcvCandle>> GetRecentCandlesAsync(string symbol, int limit, CancellationToken cancellationToken);
}
