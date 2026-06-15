using StreamPulse.Gateway.Application.Models;

namespace StreamPulse.Gateway.Application.Interfaces;

public interface IRedisTickCache
{
    Task SetTickAsync(LiveTick tick, CancellationToken ct = default);
    Task<LiveTick?> GetTickAsync(string symbol, CancellationToken ct = default);
    Task<IEnumerable<LiveTick>> GetAllTicksAsync(CancellationToken ct = default);
}
