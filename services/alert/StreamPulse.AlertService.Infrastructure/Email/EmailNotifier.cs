using MailKit.Net.Smtp;
using MailKit.Security;
using Microsoft.Extensions.Logging;
using MimeKit;
using StreamPulse.AlertService.Application.Interfaces;
using StreamPulse.AlertService.Application.Models;
using StreamPulse.AlertService.Application.Settings;

namespace StreamPulse.AlertService.Infrastructure.Email;

public class EmailNotifier : IEmailNotifier
{
    private readonly EmailSettings _settings;
    private readonly ILogger<EmailNotifier> _logger;

    public EmailNotifier(EmailSettings settings, ILogger<EmailNotifier> logger)
    {
        _settings = settings;
        _logger = logger;
    }

    public async Task SendAlertEmailAsync(AnomalyAlert alert, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(_settings.SenderEmail) ||
            _settings.SenderEmail.Equals("disabled", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogInformation("[EMAIL] Email disabled — skipping HIGH alert for {Symbol}", alert.Symbol);
            return;
        }

        try
        {
            var message = new MimeMessage();
            message.From.Add(MailboxAddress.Parse(_settings.SenderEmail));
            message.To.Add(MailboxAddress.Parse(_settings.RecipientEmail));
            message.Subject = $"[StreamPulse ALERT] HIGH {alert.AlertType} — {alert.Symbol}";

            message.Body = new TextPart("plain")
            {
                Text = $"""
                    StreamPulse Anomaly Alert
                    ─────────────────────────
                    Symbol:     {alert.Symbol}
                    Alert Type: {alert.AlertType}
                    Severity:   HIGH
                    Price:      ${alert.Price}
                    Change:     {alert.ChangePct}%
                    Volume:     {alert.Volume}
                    Score:      {alert.AnomalyScore:F4}
                    Time:       {alert.Timestamp:O}
                    ─────────────────────────
                    This is an automated alert from StreamPulse.
                    """
            };

            using var smtp = new SmtpClient();
            await smtp.ConnectAsync(_settings.SmtpHost, _settings.SmtpPort, SecureSocketOptions.StartTls, ct);
            await smtp.AuthenticateAsync(_settings.SenderEmail, _settings.SenderPassword, ct);
            await smtp.SendAsync(message, ct);
            await smtp.DisconnectAsync(true, ct);

            _logger.LogInformation("[EMAIL] Sent HIGH alert email for {Symbol}", alert.Symbol);
        }
        catch (Exception ex)
        {
            _logger.LogError("[EMAIL] Failed to send alert email for {Symbol}: {Error}", alert.Symbol, ex.Message);
        }
    }
}
