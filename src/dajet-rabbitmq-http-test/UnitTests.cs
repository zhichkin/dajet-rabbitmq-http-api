using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DaJet.RabbitMQ.HttpApi.Test
{
    [TestClass] public sealed class UnitTests
    {
        private const string VHOST = "test";
        private const string VHOST_DESCRIPTION = "test virtual host";
        private const string ROUTING_KEY = "#";
        private const string QUEUE = "test-queue";
        private const string EXCHANGE = "test-exchange";
        private const string EXCHANGE_SOURCE = "test-exchange-source";
        private const string EXCHANGE_TARGET = "test-exchange-target";

        private readonly IRabbitMQHttpManager manager = new RabbitMQHttpManager();
        public UnitTests()
        {
            manager
                .UseHostName("localhost")
                .UseUserName("guest")
                .UsePassword("guest");
        }
        [TestMethod] public async Task VirtualHost_Create()
        {
            await manager.CreateVirtualHost(VHOST, VHOST_DESCRIPTION);

            VirtualHostInfo info = await manager.GetVirtualHost(VHOST);

            Assert.AreEqual(VHOST, info.Name);
            Assert.AreEqual(VHOST_DESCRIPTION, info.Description);
        }
        [TestMethod] public async Task VirtualHost_List()
        {
            List<VirtualHostInfo> list = await manager.GetVirtualHosts();

            foreach (VirtualHostInfo info in list)
            {
                Console.WriteLine(info.Name + " [" + info.Description + "]");
            }
        }
        [TestMethod] public async Task VirtualHost_Delete()
        {
            string name = "test";

            try
            {
                await manager.DeleteVirtualHost(name);
            }
            catch (Exception error)
            {
                Assert.IsTrue(error.Message.Contains("Not Found"));
            }

            try
            {
                VirtualHostInfo info = await manager.GetVirtualHost(name);
            }
            catch (Exception error)
            {
                Assert.IsTrue(error.Message.Contains("Not found"));
            }
        }
        [TestMethod] public async Task VirtualHost_GetBindings()
        {
            // The default exchange is implicitly bound to every queue, with a routing key equal to the queue name.
            // It is not possible to explicitly bind to, or unbind from the default exchange.
            // It also cannot be deleted.

          List<BindingInfo> list = await manager.GetBindings();

            foreach (BindingInfo info in list)
            {
                Console.WriteLine((string.IsNullOrEmpty(info.Source) ? "(AMQP default)" : info.Source)
                    + " > [" + info.RoutingKey + "] > "
                    + info.Destination + " (" + info.DestinationType + ")");
            }
        }

        [TestMethod] public async Task Exchange_Create()
        {
            manager.UseVirtualHost(VHOST);

            await manager.CreateExchange(EXCHANGE);

            ExchangeInfo info = await manager.GetExchange(EXCHANGE);

            Assert.AreEqual(EXCHANGE, info.Name);
        }
        [TestMethod] public async Task Exchange_List()
        {
            manager.UseVirtualHost(VHOST);

            List<ExchangeInfo> list = await manager.GetExchanges();

            foreach (ExchangeInfo info in list)
            {
                Console.WriteLine(info.Name + " [" + info.Durable + "]");
            }
        }
        [TestMethod] public async Task Exchange_Delete()
        {
            manager.UseVirtualHost(VHOST);

            try
            {
                await manager.SafeDeleteExchange(EXCHANGE);
            }
            catch (Exception error)
            {
                Assert.IsTrue(error.Message.Contains("Not Found"));
            }

            ExchangeInfo info = await manager.GetExchange(EXCHANGE);

            Assert.IsNull(info);
        }

        [TestMethod] public async Task Queue_Create()
        {
            manager.UseVirtualHost(VHOST);

            await manager.CreateQueue(QUEUE);

            QueueInfo info = await manager.GetQueue(QUEUE);

            Assert.AreEqual(QUEUE, info.Name);
        }
        [TestMethod] public async Task Queue_List()
        {
            manager.UseVirtualHost(VHOST);

            List<QueueInfo> list = await manager.GetQueues();

            foreach (QueueInfo info in list)
            {
                Console.WriteLine(info.Name + " [" + info.Type + " : " + info.Durable + "]");
            }
        }
        [TestMethod] public async Task Queue_Delete()
        {
            manager.UseVirtualHost(VHOST);

            try
            {
                await manager.SafeDeleteQueue(QUEUE);
            }
            catch (Exception error)
            {
                Assert.IsTrue(error.Message.Contains("Not Found"));
            }

            QueueInfo info = await manager.GetQueue(QUEUE);

            Assert.IsNull(info);
        }

        [TestMethod] public async Task Binding_Exchange_Exchange_Create()
        {
            manager.UseVirtualHost(VHOST);

            VirtualHostInfo vhost = await manager.GetVirtualHost(VHOST);
            if (vhost == null)
            {
                await manager.CreateVirtualHost(VHOST, VHOST_DESCRIPTION);
            }
            vhost = await manager.GetVirtualHost(VHOST);

            Assert.AreEqual(VHOST, vhost.Name);

            ExchangeInfo source = await manager.GetExchange(EXCHANGE_SOURCE);
            if (source == null)
            {
                await manager.CreateExchange(EXCHANGE_SOURCE);
                source = await manager.GetExchange(EXCHANGE_SOURCE);
            }
            Assert.AreEqual(EXCHANGE_SOURCE, source.Name);

            ExchangeInfo target = await manager.GetExchange(EXCHANGE_TARGET);
            if (target == null)
            {
                await manager.CreateExchange(EXCHANGE_TARGET);
                target = await manager.GetExchange(EXCHANGE_TARGET);
            }
            Assert.AreEqual(EXCHANGE_TARGET, target.Name);

            await manager.CreateBinding(source, target, ROUTING_KEY);

            List<BindingInfo> bindings = await manager.GetBindings(source);
            BindingInfo binding = bindings.Where(b => b.Source == EXCHANGE_SOURCE).FirstOrDefault();

            Assert.IsNotNull(binding);
            Assert.AreEqual(VHOST, binding.VirtualHost);
            Assert.AreEqual(EXCHANGE_SOURCE, binding.Source);
            Assert.AreEqual(EXCHANGE_TARGET, binding.Destination);
            Assert.AreEqual(ROUTING_KEY, binding.RoutingKey);
            Assert.AreEqual("exchange", binding.DestinationType);
        }
        [TestMethod] public async Task Binding_Exchange_Exchange_List()
        {
            manager.UseVirtualHost(VHOST);

            ExchangeInfo exchange = await manager.GetExchange(EXCHANGE_SOURCE);

            List<BindingInfo> list = await manager.GetBindings(exchange);

            foreach (BindingInfo info in list)
            {
                Console.WriteLine(info.Source + " > [" + info.RoutingKey + "] > " + info.Destination );
            }
        }
        [TestMethod] public async Task Binding_Exchange_Exchange_Delete()
        {
            manager.UseVirtualHost(VHOST);

            // delete exchange and all of its bindings where it is source
            // this does not delete destination objects (exchanges or queues)
            // await manager.DeleteExchange(EXCHANGE_SOURCE);

            ExchangeInfo exchange = await manager.GetExchange(EXCHANGE_SOURCE);

            List<BindingInfo> bindings = await manager.GetBindings(exchange);

            foreach (BindingInfo binding in bindings)
            {
                await manager.DeleteBinding(binding);
            }

            bindings = await manager.GetBindings(exchange);

            Assert.AreEqual(0, bindings.Count);

            // clean up
            await manager.DeleteExchange(EXCHANGE_SOURCE);
            await manager.DeleteExchange(EXCHANGE_TARGET);
        }

        [TestMethod] public async Task Binding_Exchange_Queue_Create()
        {
            manager.UseVirtualHost(VHOST);

            VirtualHostInfo vhost = await manager.GetVirtualHost(VHOST);
            if (vhost == null)
            {
                await manager.CreateVirtualHost(VHOST, VHOST_DESCRIPTION);
            }
            vhost = await manager.GetVirtualHost(VHOST);

            Assert.AreEqual(VHOST, vhost.Name);

            ExchangeInfo source = await manager.GetExchange(EXCHANGE_SOURCE);
            if (source == null)
            {
                await manager.CreateExchange(EXCHANGE_SOURCE);
                source = await manager.GetExchange(EXCHANGE_SOURCE);
            }
            Assert.AreEqual(EXCHANGE_SOURCE, source.Name);

            QueueInfo target = await manager.GetQueue(QUEUE);
            if (target == null)
            {
                await manager.CreateQueue(QUEUE);
                target = await manager.GetQueue(QUEUE);
            }
            Assert.AreEqual(QUEUE, target.Name);

            await manager.CreateBinding(source, target, ROUTING_KEY);

            List<BindingInfo> bindings = await manager.GetBindings(source);
            BindingInfo binding = bindings.Where(b => b.Source == EXCHANGE_SOURCE).FirstOrDefault();

            Assert.IsNotNull(binding);
            Assert.AreEqual(VHOST, binding.VirtualHost);
            Assert.AreEqual(EXCHANGE_SOURCE, binding.Source);
            Assert.AreEqual(QUEUE, binding.Destination);
            Assert.AreEqual(ROUTING_KEY, binding.RoutingKey);
            Assert.AreEqual("queue", binding.DestinationType);
        }
        [TestMethod] public async Task Binding_Exchange_Queue_List()
        {
            manager.UseVirtualHost(VHOST);

            ExchangeInfo exchange = await manager.GetExchange(EXCHANGE_SOURCE);

            List<BindingInfo> list = await manager.GetBindings(exchange);

            foreach (BindingInfo info in list)
            {
                Console.WriteLine(info.Source + " > [" + info.RoutingKey + "] > " + info.Destination);
            }
        }
        [TestMethod] public async Task Binding_Exchange_Queue_Delete()
        {
            manager.UseVirtualHost(VHOST);

            // delete exchange and all of its bindings where it is source
            // this does not delete destination objects (exchanges or queues)
            // await manager.DeleteExchange(EXCHANGE_SOURCE);

            ExchangeInfo exchange = await manager.GetExchange(EXCHANGE_SOURCE);

            List<BindingInfo> bindings = await manager.GetBindings(exchange);

            foreach (BindingInfo binding in bindings)
            {
                await manager.DeleteBinding(binding);
            }

            bindings = await manager.GetBindings(exchange);

            Assert.AreEqual(0, bindings.Count);

            // clean up
            await manager.DeleteExchange(EXCHANGE_SOURCE);
            await manager.DeleteQueue(QUEUE);
        }

        [TestMethod] public async Task BindQueuesToAggregator()
        {
            string exchangeName = "РИБ.АПО";
            string queueFilter = @"^РИБ[.][0-9]+[.]ЦБ$";

            ExchangeInfo exchange = await manager.GetExchange(exchangeName);
            if (exchange == null)
            {
                Console.WriteLine($"Exchange \"{exchangeName}\" is not found.");
                return;
            }

            int page = 1;
            int size = 100;
            QueueResponse response = await manager.GetQueues(page, size, queueFilter);
            if (response == null || response.FilteredCount == 0)
            {
                Console.WriteLine("Queues are not found.");
                return;
            }
            int pageCount = response.PageCount;

            while (page <= pageCount)
            {
                Console.WriteLine($"Page # {response.Page} of total {response.PageCount} pages (size = {response.PageSize})");
                Console.WriteLine($"Items count: {response.ItemCount}");
                Console.WriteLine($"Total items count: {response.TotalCount}");
                Console.WriteLine($"Filtered items count: {response.FilteredCount}");

                for (int i = 0; i < response.Items.Count; i++)
                {
                    QueueInfo queue = response.Items[i];

                    Console.WriteLine($"{(i + 1)}. Queue \"{queue.Name}\" ({(queue.Durable ? "durable" : "transient")})");

                    List<BindingInfo> bindings = await manager.GetBindings(queue);

                    bool exists = false;

                    foreach (BindingInfo binding in bindings)
                    {
                        if (binding.Source == exchangeName)
                        {
                            exists = true;
                            Console.WriteLine($" - [{binding.Source}] -> [{binding.Destination}] ({binding.RoutingKey}) {binding.PropertiesKey}");
                            break;
                        }
                    }

                    if (!exists)
                    {
                        string routingKey = queue.Name.Split('.')[1];
                        await manager.CreateBinding(exchange, queue, routingKey);
                        Console.WriteLine($" + [{exchange.Name}] -> [{queue.Name}] ({routingKey})");
                    }
                }

                page++;

                if (page <= pageCount)
                {
                    response = await manager.GetQueues(page, size, queueFilter);
                }
            }
        }
        [TestMethod] public async Task BindQueuesToDistributor()
        {
            string exchangeName = "РИБ.ERP";
            string queueFilter = @"^РИБ[.]ЦБ[.][0-9]+$";

            ExchangeInfo exchange = await manager.GetExchange(exchangeName);
            if (exchange == null)
            {
                Console.WriteLine($"Exchange \"{exchangeName}\" is not found.");
                return;
            }

            int page = 1;
            int size = 100;
            QueueResponse response = await manager.GetQueues(page, size, queueFilter);
            if (response == null || response.FilteredCount == 0)
            {
                Console.WriteLine("Queues are not found.");
                return;
            }
            int pageCount = response.PageCount;

            while (page <= pageCount)
            {
                Console.WriteLine($"Page # {response.Page} of total {response.PageCount} pages (size = {response.PageSize})");
                Console.WriteLine($"Items count: {response.ItemCount}");
                Console.WriteLine($"Total items count: {response.TotalCount}");
                Console.WriteLine($"Filtered items count: {response.FilteredCount}");

                for (int i = 0; i < response.Items.Count; i++)
                {
                    QueueInfo queue = response.Items[i];

                    Console.WriteLine($"{(i + 1)}. Queue \"{queue.Name}\" ({(queue.Durable ? "durable" : "transient")})");

                    List<BindingInfo> bindings = await manager.GetBindings(queue);

                    bool exists = false;

                    foreach (BindingInfo binding in bindings)
                    {
                        if (binding.Source == exchangeName)
                        {
                            exists = true;
                            Console.WriteLine($" - [{binding.Source}] -> [{binding.Destination}] ({binding.RoutingKey}) {binding.PropertiesKey}");
                            break;
                        }
                    }

                    if (!exists)
                    {
                        string routingKey = queue.Name.Split('.')[2];
                        await manager.CreateBinding(exchange, queue, routingKey);
                        Console.WriteLine($" + [{exchange.Name}] -> [{queue.Name}] ({routingKey})");
                    }
                }

                page++;

                if (page <= pageCount)
                {
                    response = await manager.GetQueues(page, size, queueFilter);
                }
            }
        }
    }
}