using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using StreamPulse.Gateway.Application.Interfaces;
using StreamPulse.Gateway.Application.Models;
using StreamPulse.Gateway.Application.Settings;

namespace StreamPulse.Gateway.Infrastructure;

public class RedisTickCache : IRedisTickCache
{
    private readonly IDatabase _db;
    private readonly ILogger<RedisTickCache> _logger;
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);
    private static readonly JsonSerializerOptions _json = new() { PropertyNameCaseInsensitive = true };

    public RedisTickCache(IConnectionMultiplexer redis, ILogger<RedisTickCache> logger)
    {
        _db = redis.GetDatabase();
        _logger = logger;
    }

    public async Task SetTickAsync(LiveTick tick, CancellationToken ct = default)
    {
        var key = $"streampulse:tick:{tick.Symbol}";
        var value = JsonSerializer.Serialize(tick);
        await _db.StringSetAsync(key, value, Ttl);
    }

    public async Task<LiveTick?> GetTickAsync(string symbol, CancellationToken ct = default)
    {
        var key = $"streampulse:tick:{symbol}";
        var value = await _db.StringGetAsync(key);
        if (value.IsNullOrEmpty) return null;
        return JsonSerializer.Deserialize<LiveTick>((string)value!, _json);
    }

    public async Task<IEnumerable<LiveTick>> GetAllTicksAsync(CancellationToken ct = default)
    {
        var server = _db.Multiplexer.GetServer(_db.Multiplexer.GetEndPoints()[0]);
        var keys = server.Keys(pattern: "streampulse:tick:*").ToArray();
        var ticks = new List<LiveTick>();
        foreach (var key in keys)
        {
            var value = await _db.StringGetAsync(key);
            if (!value.IsNullOrEmpty)
            {
                var tick = JsonSerializer.Deserialize<LiveTick>((string)value!, _json);
                if (tick != null) ticks.Add(tick);
            }
        }
        return ticks;
    }
}
