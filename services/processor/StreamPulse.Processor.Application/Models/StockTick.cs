namespace StreamPulse.Processor.Application.Models;

public sealed class StockTick
{
    public string Symbol { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Volume { get; set; }
    public DateTimeOffset Timestamp { get; set; }
    public decimal ChangePct { get; set; }
}
