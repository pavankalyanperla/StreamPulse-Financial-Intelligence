namespace StreamPulse.Processor.Application.Models;

public sealed class OhlcvCandle
{
    public string Symbol { get; set; } = string.Empty;
    public decimal OpenPrice { get; set; }
    public decimal HighPrice { get; set; }
    public decimal LowPrice { get; set; }
    public decimal ClosePrice { get; set; }
    public long TotalVolume { get; set; }
    public DateTimeOffset CandleTime { get; set; }
    public int TickCount { get; set; }
}
