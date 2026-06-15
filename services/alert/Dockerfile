FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY StreamPulse.AlertService.Application/StreamPulse.AlertService.Application.csproj StreamPulse.AlertService.Application/
COPY StreamPulse.AlertService.Infrastructure/StreamPulse.AlertService.Infrastructure.csproj StreamPulse.AlertService.Infrastructure/
COPY StreamPulse.AlertService/StreamPulse.AlertService.csproj StreamPulse.AlertService/

RUN dotnet restore StreamPulse.AlertService/StreamPulse.AlertService.csproj

COPY . .
RUN dotnet publish StreamPulse.AlertService/StreamPulse.AlertService.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
ENV ASPNETCORE_ENVIRONMENT=Docker
COPY --from=build /app/publish .
EXPOSE 5200
ENTRYPOINT ["dotnet", "StreamPulse.AlertService.dll"]
