FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY StreamPulse.Processor.Application/StreamPulse.Processor.Application.csproj StreamPulse.Processor.Application/
COPY StreamPulse.Processor.Infrastructure/StreamPulse.Processor.Infrastructure.csproj StreamPulse.Processor.Infrastructure/
COPY StreamPulse.Processor/StreamPulse.Processor.csproj StreamPulse.Processor/
RUN dotnet restore StreamPulse.Processor/StreamPulse.Processor.csproj

COPY . .
RUN dotnet publish StreamPulse.Processor/StreamPulse.Processor.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

ENV ASPNETCORE_ENVIRONMENT=Docker

EXPOSE 5100

ENTRYPOINT ["dotnet", "StreamPulse.Processor.dll"]
