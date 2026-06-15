using StreamPulse.Processor;
using StreamPulse.Processor.Application.Interfaces;
using StreamPulse.Processor.Application.Services;
using StreamPulse.Processor.Infrastructure.Kafka;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<ICandleAggregator, CandleAggregator>();
builder.Services.AddSingleton<IKafkaProducerService, KafkaProducerService>();
builder.Services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
