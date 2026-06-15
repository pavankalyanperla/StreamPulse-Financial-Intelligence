FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY StreamPulse.Gateway.Application/StreamPulse.Gateway.Application.csproj StreamPulse.Gateway.Application/
COPY StreamPulse.Gateway.Infrastructure/StreamPulse.Gateway.Infrastructure.csproj StreamPulse.Gateway.Infrastructure/
COPY StreamPulse.Gateway/StreamPulse.Gateway.csproj StreamPulse.Gateway/

RUN dotnet restore StreamPulse.Gateway/StreamPulse.Gateway.csproj

COPY . .

RUN dotnet publish StreamPulse.Gateway/StreamPulse.Gateway.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
COPY --from=build /src/StreamPulse.Gateway/ocelot.json .

ENV ASPNETCORE_ENVIRONMENT=Docker
EXPOSE 5000

ENTRYPOINT ["dotnet", "StreamPulse.Gateway.dll"]
