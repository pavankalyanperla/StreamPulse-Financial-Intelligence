namespace StreamPulse.AlertService.Application.Settings;

public class EmailSettings
{
    public string SmtpHost { get; set; } = "smtp.gmail.com";
    public int SmtpPort { get; set; } = 587;
    public string SenderEmail { get; set; } = "disabled";
    public string SenderPassword { get; set; } = string.Empty;
    public string RecipientEmail { get; set; } = string.Empty;
}
