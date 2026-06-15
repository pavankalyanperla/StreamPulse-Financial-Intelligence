using StreamPulse.Processor.Application.Models;

namespace StreamPulse.Processor.Application.Interfaces;

public interface IKafkaProducerService
{
    Task PublishCandleAsync(OhlcvCandle candle, CancellationToken cancellationToken);
}
