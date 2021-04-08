using System;
using System.Threading;
using System.Threading.Tasks;
using ActiveMQ.Artemis.Client;
using ActiveMQ.Artemis.Client.AutoRecovering.RecoveryPolicy;
using ActiveMQ.Artemis.Client.Exceptions;
using ActiveMQ.Artemis.Client.Transactions;
using Microsoft.Extensions.Logging;

namespace ConsumerB
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, _) => { cts.Cancel(); };

            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddConsole();
            });

            var connectionFactory = new ConnectionFactory
            {
                AutomaticRecoveryEnabled = true,
                RecoveryPolicy = RecoveryPolicyFactory.ConstantBackoff(TimeSpan.FromSeconds(5), retryCount: 5),
                LoggerFactory = loggerFactory
            };
            var endpoint = Endpoint.Create(host: "localhost", port: 5672, "guest", "guest");

            await using var connection = await connectionFactory.CreateAsync(endpoint, cts.Token);
            connection.ConnectionRecoveryError += (_, _) =>
            {
                Console.WriteLine("Disconnected");
                cts.Cancel();
            };

            var address = "my-address";
            var queue = "my-queue2";

            var topologyManager = await connection.CreateTopologyManagerAsync(cts.Token);
            await topologyManager.DeclareQueueAsync(new QueueConfiguration
            {
                Address = address,
                Name = queue,
                AutoCreateAddress = true,
                RoutingType = RoutingType.Multicast,
                Exclusive = true,
                Durable = true,
            }, cts.Token);
            await topologyManager.DisposeAsync();

            var consumer = await connection.CreateConsumerAsync(address, queue, cancellationToken: cts.Token);

            Console.WriteLine($"Attached to queue: {queue}");

            await Task.Run(async () =>
            {
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        await using var transaction = new Transaction();
                        var msg = await consumer.ReceiveAsync(cts.Token);
                        await consumer.AcceptAsync(msg, transaction, cts.Token);
                        await transaction.CommitAsync(cts.Token);
                        Console.WriteLine($"Received message: {msg.GetBody<string>()}");
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (ActiveMQArtemisClientException e)
                    {
                        Console.Error.WriteLine(e);
                    }
                }
            }, cts.Token);
        }
    }
}