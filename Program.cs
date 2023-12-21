using System.Text;
using System.Text.Json;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace AzureEventHubSender
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            while (true)
            {
                var eventHubProducer = GetEventHubProducerClient();

                // send messages
                while (true)
                {
                    try
                    {
                        using var eventBatch = await eventHubProducer.CreateBatchAsync(CancellationToken.None);
                        Console.WriteLine("Please insert a message...");
                        var input = Console.ReadLine();
                        if (string.IsNullOrEmpty(input))
                        {
                            Console.WriteLine("Invalid message...");
                            continue;
                        }

                        if (input.Equals("exit", StringComparison.InvariantCultureIgnoreCase))
                        {
                            break;
                        }

                        var json = JsonDocument.Parse(input);
                        var eventData = new EventData(Encoding.UTF8.GetBytes(json.RootElement.ToString()));
                        if (eventBatch.TryAdd(eventData) is false)
                        {
                            Console.WriteLine("Couldn't add message to the batch...");
                            continue;
                        }

                        await eventHubProducer.SendAsync(eventBatch);
                        Console.WriteLine("Message sent successfully...");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }

                break;
            }
        }

        private static EventHubProducerClient GetEventHubProducerClient()
        {
            while (true)
            {
                string? connectionString;
                string? eventHubName;
                while (true)
                {
                    Console.WriteLine("Please insert a connection string...");
                    connectionString = Console.ReadLine();
                    if (string.IsNullOrEmpty(connectionString) is false)
                    {
                        break;
                    }
                    Console.WriteLine("Invalid connection string entered.");
                }

                while (true)
                {
                    Console.WriteLine("Please insert an event hub name...");
                    eventHubName = Console.ReadLine();
                    if (string.IsNullOrEmpty(eventHubName) is false)
                    {
                        break;
                    }
                    Console.WriteLine("Invalid event hub name entered.");
                }

                try
                {
                    return new EventHubProducerClient(connectionString, eventHubName);
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
    }
}