using Azure.Core;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// SENDER

// Main program logic at the top for clarity
IConfiguration configuration = SetupConfiguration(args);

var myNamespace = configuration?["EVENT_HUB_NAMESPACE"];
var myHubName = configuration?["HUB_NAME"];

Console.WriteLine($"Event Hub Namespace: {myNamespace}");
Console.WriteLine($"Event Hub Name: {myHubName}");


// I used az login --tenant <TENANT_ID> to get the token
//await GetToken();

// number of events to be sent to the event hub
int numOfEvents = 3;

// The Event Hubs client types are safe to cache and use as a singleton for the lifetime
// of the application, which is best practice when events are being published or read regularly.
EventHubProducerClient producerClient = new EventHubProducerClient(
    myNamespace,
    myHubName,
    new DefaultAzureCredential());

try
{
    for (int batchNumber = 1; batchNumber <= 3; batchNumber++) // Example: sending two batches
    {
        // Create a new batch of events
        using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

        for (int i = 1; i <= numOfEvents; i++)
        {
            if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"ReRun {i} in Batch {batchNumber}"))))
            {
                // if it is too large for the batch
                throw new Exception($"Event {i} in Batch {batchNumber} is too large for the batch and cannot be sent.");
            }
        }

        // Use the producer client to send the batch of events to the event hub
        await producerClient.SendAsync(eventBatch);
        Console.WriteLine($"A batch of {numOfEvents} events has been published in Batch {batchNumber}.");
    }

    Console.ReadLine();
}
finally
{
    await producerClient.DisposeAsync();
}

// async Task GetToken()
//{
//    // This method is used to get the token for the Event Hub
//    // The token is used to authenticate the client to the Event Hub
//    // The token is obtained using the DefaultAzureCredential class from the Azure.Identity namespace
//    // The DefaultAzureCredential class uses the Azure CLI to get the token
//    // The Azure CLI must be installed and configured on the machine running this code
//    // The Azure CLI must be logged in to the correct tenant and subscription
//    // The tenant ID can be obtained from the Azure portal or by running the az account show command in the Azure CLI


//    //try
//    //{
//    //    var credential = new DefaultAzureCredential(new DefaultAzureCredentialOptions { TenantId = { "Add Your Tenant ID" } });
//    //    var tokenRequestContext = new TokenRequestContext(new[] { "https://management.azure.com/.default" });


//    //    AccessToken token = await credential.GetTokenAsync(tokenRequestContext);

//    //    Console.WriteLine("Token acquired successfully:");
//    //    Console.WriteLine(token.Token);
//    //}
//    //catch (Exception ex)
//    //{
//    //    Console.WriteLine("Failed to acquire token:");
//    //    Console.WriteLine(ex.Message);
//    //}


//}

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