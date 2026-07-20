//It’s a high‑priority Service Bus topic handler that deserializes the message, verifies the priority, processes the order, saves it, and logs every step with full error handling and correlation tracking.
using System;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

public class ProcessHighPriorityOrders
{
    private readonly ILogger<ProcessHighPriorityOrders> _logger;
    private readonly IHighPriorityOrderService _priorityService;
    private readonly IOrderRepository _orderRepository;

    public ProcessHighPriorityOrders(
        ILogger<ProcessHighPriorityOrders> logger,
        IHighPriorityOrderService priorityService,
        IOrderRepository orderRepository)
    {
        _logger = logger;
        _priorityService = priorityService;
        _orderRepository = orderRepository;
    }

    [FunctionName("ProcessHighPriorityOrders")]
    public async Task Run(
        [ServiceBusTrigger("orders-topic", "high-priority", Connection = "ServiceBusConnection")]
        string message,
        string messageId,
        string correlationId)
    {
        _logger.LogInformation(
            "High-priority message received. MessageId={MessageId}, CorrelationId={CorrelationId}",
            messageId, correlationId);

        HighPriorityOrderDto order;

        try
        {
            // 1. Deserialize
            order = JsonSerializer.Deserialize<HighPriorityOrderDto>(message);

            if (order == null)
            {
                _logger.LogWarning(
                    "Deserialization returned null. MessageId={MessageId}, CorrelationId={CorrelationId}",
                    messageId, correlationId);
                return;
            }

            _logger.LogInformation(
                "Order deserialized. OrderId={OrderId}, Priority={Priority}",
                order.OrderId, order.Priority);

            // 2. Validate priority
            if (!order.Priority.Equals("High", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning(
                    "Message routed to high-priority subscription but priority is not high. OrderId={OrderId}, Priority={Priority}",
                    order.OrderId, order.Priority);
                return;
            }

            // 3. Domain processing
            _logger.LogInformation(
                "Processing high-priority order. OrderId={OrderId}, CustomerId={CustomerId}",
                order.OrderId, order.CustomerId);

            var processedOrder = await _priorityService.ProcessAsync(order);

            // 4. Persistence
            _logger.LogInformation(
                "Saving high-priority order. OrderId={OrderId}",
                order.OrderId);

            await _orderRepository.SaveAsync(processedOrder);

            // 5. Telemetry
            _logger.LogInformation(
                "High-priority order processed successfully. OrderId={OrderId}",
                order.OrderId);
        }
        catch (JsonException jsonEx)
        {
            _logger.LogError(
                jsonEx,
                "JSON parsing error. MessageId={MessageId}, CorrelationId={CorrelationId}",
                messageId, correlationId);

            // Optionally send to DLQ
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Unexpected error while processing high-priority order. MessageId={MessageId}, CorrelationId={CorrelationId}",
                messageId, correlationId);

            // Throw to trigger retry
            throw;
        }
    }
}

public record HighPriorityOrderDto(
    string OrderId,
    string CustomerId,
    string Priority,
    decimal Amount);
