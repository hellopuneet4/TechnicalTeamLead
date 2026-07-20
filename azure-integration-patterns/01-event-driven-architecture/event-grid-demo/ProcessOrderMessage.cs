//THIS IS DEMO FOR PROCESSING MESSAGE USING SERVICE BUS MESSAGE QUEUE in AZURE using C# .Net
using System;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

public class ProcessOrderMessage
{
    private readonly ILogger<ProcessOrderMessage> _logger;
    private readonly IOrderProcessor _orderProcessor;
    private readonly IOrderRepository _orderRepository;

    public ProcessOrderMessage(
        ILogger<ProcessOrderMessage> logger,
        IOrderProcessor orderProcessor,
        IOrderRepository orderRepository)
    {
        _logger = logger;
        _orderProcessor = orderProcessor;
        _orderRepository = orderRepository;
    }

    [FunctionName("ProcessOrderMessage")]
    public async Task Run(
        [ServiceBusTrigger("orders-queue", Connection = "ServiceBusConnection")]
        string message,
        string messageId,
        string correlationId)
    {
        _logger.LogInformation("Received message. MessageId={MessageId}, CorrelationId={CorrelationId}", 
            messageId, correlationId);

        OrderDto order;

        try
        {
            // 1. Deserialize
            order = JsonSerializer.Deserialize<OrderDto>(message);
            if (order == null)
            {
                _logger.LogWarning("Message deserialization returned null. MessageId={MessageId}", messageId);
                return;
            }

            _logger.LogInformation("Order deserialized successfully. OrderId={OrderId}", order.OrderId);

            // 2. Validate
            var validationResult = OrderValidator.Validate(order);
            if (!validationResult.IsValid)
            {
                _logger.LogWarning("Order validation failed. OrderId={OrderId}, Errors={Errors}",
                    order.OrderId, string.Join(", ", validationResult.Errors));

                return;
            }

            // 3. Domain Processing
            _logger.LogInformation("Processing order. OrderId={OrderId}", order.OrderId);
            var processedOrder = await _orderProcessor.ProcessAsync(order);

            // 4. Persist
            _logger.LogInformation("Persisting order. OrderId={OrderId}", order.OrderId);
            await _orderRepository.SaveAsync(processedOrder);

            // 5. Telemetry
            _logger.LogInformation("Order processed successfully. OrderId={OrderId}", order.OrderId);
        }
        catch (JsonException jsonEx)
        {
            _logger.LogError(jsonEx, 
                "JSON parsing error. MessageId={MessageId}, CorrelationId={CorrelationId}", 
                messageId, correlationId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, 
                "Unexpected error while processing order. MessageId={MessageId}, CorrelationId={CorrelationId}", 
                messageId, correlationId);
            throw;
        }
    }
}

public record OrderDto(string OrderId, string CustomerId, decimal Amount);

public static class OrderValidator
{
    public static (bool IsValid, string[] Errors) Validate(OrderDto order)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(order.OrderId))
            errors.Add("OrderId is required.");

        if (order.Amount <= 0)
            errors.Add("Amount must be greater than zero.");

        return (errors.Count == 0, errors.ToArray());
    }
}

public interface IOrderProcessor
{
    Task<OrderDto> ProcessAsync(OrderDto order);
}

public interface IOrderRepository
{
    Task SaveAsync(OrderDto order);
}
