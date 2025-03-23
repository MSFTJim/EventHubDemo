using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// RECEIVER

// Main program logic at the top for clarity
IConfiguration configuration = SetupConfiguration(args);

var myNamespace = configuration?["EVENT_HUB_NAMESPACE"];
var myHubName = configuration?["HUB_NAME"];
var mystorageAccountName = configuration?["STORAGE_ACCOUNT_NAME"];
var myblobContainerName = configuration?["BLOB_CONTAINER_NAME"];

Console.WriteLine($"Event Hub Namespace: {myNamespace}");
Console.WriteLine($"Event Hub Name: {myHubName}");
Console.WriteLine($"Storage Account Name: {mystorageAccountName}");
Console.WriteLine($"Blob Container Name: {myblobContainerName}");

// Create a blob container client that the event processor will use
// TODO: Replace <STORAGE_ACCOUNT_NAME> and <BLOB_CONTAINER_NAME> with actual names
BlobContainerClient storageClient = new BlobContainerClient(
    new Uri($"https://{mystorageAccountName}.blob.core.windows.net/{myblobContainerName}"),    
    new DefaultAzureCredential());

// Verify the storage client by attempting to retrieve the container properties
try
{
    var properties = await storageClient.GetPropertiesAsync();
    Console.WriteLine("Successfully accessed the blob container.");
}
catch (Exception ex)
{
    Console.WriteLine($"Failed to access the blob container: {ex.Message}");
    return; // Exit if the storage client is not properly configured
}



// Create an event processor client to process events in the event hub
// TODO: Replace the <EVENT_HUBS_NAMESPACE> and <HUB_NAME> placeholder values
var processor = new EventProcessorClient(
    storageClient,
    EventHubConsumerClient.DefaultConsumerGroupName,
    myNamespace,
    myHubName,
    new DefaultAzureCredential());

// Register handlers for processing events and handling errors
processor.ProcessEventAsync += ProcessEventHandler;
processor.ProcessErrorAsync += ProcessErrorHandler;

// Start the processing
await processor.StartProcessingAsync();

// Wait for 30 seconds for the events to be processed
await Task.Delay(TimeSpan.FromSeconds(30));

// Stop the processing
await processor.StopProcessingAsync();

Task ProcessEventHandler(ProcessEventArgs eventArgs)
{
    // Write the body of the event to the console window
    Console.WriteLine("\tReceived event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));
    Console.ReadLine();
    return Task.CompletedTask;
}

Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
{
    // Write details about the error to the console window
    Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
    Console.WriteLine(eventArgs.Exception.Message);
    Console.ReadLine();
    return Task.CompletedTask;
}
// Method containing the configuration setup logic
static IConfiguration SetupConfiguration(string[] args)
{
    // Create a host builder and explicitly set the environment to Development
    var builder = Host.CreateDefaultBuilder(args)
        .UseEnvironment("Development");

    // Configure configuration to include user secrets
    builder.ConfigureAppConfiguration((hostContext, config) =>
    {
        // Add user secrets when in development environment
        if (hostContext.HostingEnvironment.IsDevelopment())
        {
            config.AddUserSecrets<Program>();
        }
    });

    // Build the host
    var host = builder.Build();

    // Access configuration directly without DI and ensure it's not null
    return host.Services.GetRequiredService<IConfiguration>();
}