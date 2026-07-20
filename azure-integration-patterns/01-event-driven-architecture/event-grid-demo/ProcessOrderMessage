public class ProcessOrderMessage
{
    private readonly ILogger<ProcessOrderMessage> _logger;

    public ProcessOrderMessage(ILogger<ProcessOrderMessage> logger)
    {
        _logger = logger;
    }

    [FunctionName("ProcessOrderMessage")]
    public void Run(
        [ServiceBusTrigger("orders-queue", Connection = "ServiceBusConnection")]
        string message)
    {
        _logger.LogInformation($"Received Order: {message}");
        // Deserialize → Validate → Process → Persist
    }
}
