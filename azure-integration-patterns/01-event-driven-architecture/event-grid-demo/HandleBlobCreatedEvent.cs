//It’s simply an Azure Function that reacts to an Event Grid BlobCreated event, extracts the blob’s URL, and logs that a new blob was created.
using System;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Extensions.Logging;
using Azure.Identity;

public class HandleBlobCreatedEvent
{
    private readonly ILogger<HandleBlobCreatedEvent> _logger;

    public HandleBlobCreatedEvent(ILogger<HandleBlobCreatedEvent> logger)
    {
        _logger = logger;
    }

    [FunctionName("HandleBlobCreatedEvent")]
    public async Task Run(
        [EventGridTrigger] EventGridEvent eventGridEvent,
        ILogger log)
    {
        log.LogInformation(
            "Event received. EventType={EventType}, Id={Id}, Subject={Subject}",
            eventGridEvent.EventType,
            eventGridEvent.Id,
            eventGridEvent.Subject);

        try
        {
            // 1. Extract blob URL
            var blobData = eventGridEvent.Data.ToObjectFromJson<BlobCreatedEventData>();
            var blobUrl = blobData.Url;

            log.LogInformation("Blob created at URL: {BlobUrl}", blobUrl);

            // 2. Parse blob container + name
            var blobUri = new Uri(blobUrl);
            var blobClient = new BlobClient(blobUri, new DefaultAzureCredential());

            log.LogInformation(
                "Parsed Blob URI. Container={Container}, BlobName={BlobName}",
                blobClient.BlobContainerName,
                blobClient.Name);

            // 3. Read blob metadata
            var properties = await blobClient.GetPropertiesAsync();
            var contentType = properties.Value.ContentType;
            var contentLength = properties.Value.ContentLength;

            log.LogInformation(
                "Blob metadata retrieved. ContentType={ContentType}, Size={SizeBytes}",
                contentType,
                contentLength);

            // 4. Optional: Read blob content
            var download = await blobClient.DownloadContentAsync();
            var blobContent = download.Value.Content.ToString();

            log.LogInformation(
                "Blob content read successfully. Length={Length}",
                blobContent.Length);

            // 5. Domain processing (example)
            await ProcessBlobAsync(blobClient, blobContent, log);

            // 6. Telemetry
            log.LogInformation(
                "Blob processing completed successfully. BlobName={BlobName}",
                blobClient.Name);
        }
        catch (Exception ex)
        {
            log.LogError(
                ex,
                "Error processing BlobCreated event. EventId={EventId}, Subject={Subject}",
                eventGridEvent.Id,
                eventGridEvent.Subject);

            // Throw to allow retry or dead-letter routing
            throw;
        }
    }

    private async Task ProcessBlobAsync(BlobClient blobClient, string content, ILogger log)
    {
        // Example domain logic
        log.LogInformation(
            "Processing blob content for BlobName={BlobName}",
            blobClient.Name);

        // Add your business logic here
        await Task.CompletedTask;
    }
}
