using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Web;

// https://rawcdn.githack.com/rabbitmq/rabbitmq-server/v3.8.19/deps/rabbitmq_management/priv/www/api/index.html

namespace DaJet.RabbitMQ.HttpApi
{
    public interface IRabbitMQHttpManager : IDisposable
    {
        #region "Configure HTTP connection"

        IRabbitMQHttpManager UseHostName(string hostName);
        IRabbitMQHttpManager UsePortNumber(int portNumber);
        IRabbitMQHttpManager UseVirtualHost(string vhost);
        IRabbitMQHttpManager UseUserName(string userName);
        IRabbitMQHttpManager UsePassword(string password);

        string HostName { get; }
        int PortNumber { get; }
        string VirtualHost { get; }
        string UserName { get; }

        #endregion

        #region "VHOST"

        Task<List<VirtualHostInfo>> GetVirtualHosts();
        Task<VirtualHostInfo> GetVirtualHost(string name);
        Task CreateVirtualHost(string name, string description = "");
        Task DeleteVirtualHost(string name);

        #endregion

        #region "EXCHANGE"

        Task<List<ExchangeInfo>> GetExchanges();
        Task<ExchangeResponse> GetExchanges(int page, int size, string regex);
        Task<ExchangeInfo> GetExchange(string name);
        Task CreateExchange(string name);
        Task DeleteExchange(string name);
        Task SafeDeleteExchange(string name);

        #endregion

        #region "QUEUE"

        Task<List<QueueInfo>> GetQueues();
        Task<QueueResponse> GetQueues(int page, int size, string regex);
        Task<QueueInfo> GetQueue(string name);
        Task CreateQueue(string name);
        Task DeleteQueue(string name);
        Task SafeDeleteQueue(string name);

        #endregion

        #region "BINDING"

        Task<List<BindingInfo>> GetBindings();
        Task<List<BindingInfo>> GetBindings(QueueInfo queue);
        Task<List<BindingInfo>> GetBindings(ExchangeInfo exchange);
        Task CreateBinding(ExchangeInfo exchange, QueueInfo queue, string routingKey);
        Task CreateBinding(ExchangeInfo source, ExchangeInfo destination, string routingKey);
        Task DeleteBinding(BindingInfo binding);

        #endregion
    }
    public sealed class RabbitMQHttpManager : IRabbitMQHttpManager
    {
        private HttpClient HttpClient { get; set; } = new HttpClient();
        public RabbitMQHttpManager() { ConfigureHttpClient(); }
        public void Dispose() { HttpClient?.Dispose(); }

        #region "CONFIGURATION HttpClient"

        private void ConfigureHttpClient()
        {
            HttpClient.BaseAddress = new Uri($"http://{HostName}:{PortNumber}");
            byte[] authToken = Encoding.ASCII.GetBytes($"{UserName}:{Password}");
            HttpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(authToken));
        }

        public string HostName { get; private set; } = "localhost";
        public int PortNumber { get; private set; } = 15672;
        public string UserName { get; private set; } = "guest";
        private string Password { get; set; } = "guest";
        public string VirtualHost { get; private set; } = "/";

        public IRabbitMQHttpManager UseHostName(string hostName)
        {
            HostName = hostName;
            ConfigureHttpClient();
            return this;
        }
        public IRabbitMQHttpManager UsePortNumber(int portNumber)
        {
            PortNumber = portNumber;
            ConfigureHttpClient();
            return this;
        }
        public IRabbitMQHttpManager UseUserName(string userName)
        {
            UserName = userName;
            ConfigureHttpClient();
            return this;
        }
        public IRabbitMQHttpManager UsePassword(string password)
        {
            Password = password;
            ConfigureHttpClient();
            return this;
        }
        public IRabbitMQHttpManager UseVirtualHost(string vhost)
        {
            VirtualHost = vhost;
            return this;
        }

        #endregion

        #region "VHOST"

        public async Task<List<VirtualHostInfo>> GetVirtualHosts()
        {
            string url = $"/api/vhosts";
            HttpResponseMessage response = await HttpClient.GetAsync(url);
            Stream stream = await response.Content.ReadAsStreamAsync();
            List<VirtualHostInfo> list = await JsonSerializer.DeserializeAsync<List<VirtualHostInfo>>(stream);
            return list;
        }
        public async Task<VirtualHostInfo> GetVirtualHost(string name)
        {
            string url = $"/api/vhosts/{name}";
            HttpResponseMessage response = await HttpClient.GetAsync(url);
            Stream stream = await response.Content.ReadAsStreamAsync();
            VirtualHostInfo vhost = await JsonSerializer.DeserializeAsync<VirtualHostInfo>(stream);
            return (response.StatusCode == HttpStatusCode.OK ? vhost : null);
        }
        public async Task CreateVirtualHost(string name, string description = "")
        {
            string url = $"/api/vhosts/{name}";
            VirtualHostInfo vhost = new VirtualHostInfo()
            {
                Description = description
            };
            HttpResponseMessage response = await HttpClient.PutAsJsonAsync(url, vhost);
            if (response.StatusCode != HttpStatusCode.Created)
            {
                throw new Exception(response.ReasonPhrase); // No Content
            }
        }
        public async Task DeleteVirtualHost(string name)
        {
            string url = $"/api/vhosts/{name}";
            HttpResponseMessage response = await HttpClient.DeleteAsync(url);
            if (response.StatusCode != HttpStatusCode.NoContent)
            {
                throw new Exception(response.ReasonPhrase); // Not Found
            }
        }

        #endregion

        #region "EXCHANGE"

        public async Task<List<ExchangeInfo>> GetExchanges()
        {
            string url = $"/api/exchanges/{HttpUtility.UrlEncode(VirtualHost)}";
            HttpResponseMessage response = await HttpClient.GetAsync(url);
            Stream stream = await response.Content.ReadAsStreamAsync();
            List<ExchangeInfo> list = await JsonSerializer.DeserializeAsync<List<ExchangeInfo>>(stream);
            return list;
        }
        public async Task<ExchangeResponse> GetExchanges(int page, int size, string regex)
        {
            if (size == 0 || size > 100)
            {
                size = 100;
            }
            string url = $"/api/exchanges/{HttpUtility.UrlEncode(VirtualHost)}?page={page}&page_size={size}&name={HttpUtility.UrlEncode(regex)}&use_regex=true";
            HttpResponseMessage response = await HttpClient.GetAsync(url);
            Stream stream = await response.Content.ReadAsStreamAsync();
            ExchangeResponse result = await JsonSerializer.DeserializeAsync<ExchangeResponse>(stream);
            return result;
        }
        public async Task<ExchangeInfo> GetExchange(string name)
        {
            string url = $"/api/exchanges/{HttpUtility.UrlEncode(VirtualHost)}?page=1&page_size=1&name={name}";
            HttpResponseMessage response = await HttpClient.GetAsync(url);
            Stream stream = await response.Content.ReadAsStreamAsync();
            ExchangeResponse exchanges = await JsonSerializer.DeserializeAsync<ExchangeResponse>(stream);
            return (exchanges == null || exchanges.Items == null || exchanges.Items.Count == 0)
                ? null
                : exchanges.Items[0];
        }
        public async Task CreateExchange(string name)
        {
            string url = $"/api/exchanges/{HttpUtility.UrlEncode(VirtualHost)}/{name}";
            ExchangeRequest exchange = new ExchangeRequest()
            {
                Type = "topic",
                Durable = true,
                Internal = false,
                AutoDelete = false
            };
            HttpResponseMessage response = await HttpClient.PutAsJsonAsync(url, exchange);
            if (response.StatusCode != HttpStatusCode.Created)
            {
                throw new Exception(response.ReasonPhrase); // No Content | Not found for absent vhost
            }
        }
        public async Task DeleteExchange(string name)
        {
            string url = $"/api/exchanges/{HttpUtility.UrlEncode(VirtualHost)}/{name}";
            HttpResponseMessage response = await HttpClient.DeleteAsync(url);
            if (response.StatusCode != HttpStatusCode.NoContent)
            {
                throw new Exception(response.ReasonPhrase); // Not Found
            }
        }
        public async Task SafeDeleteExchange(string name)
        {
            // When DELETEing an exchange you can add the query string parameter if-unused = true.
            // This prevents the delete from succeeding if the exchange is bound to a queue or as a source to another exchange.

            string url = $"/api/exchanges/{HttpUtility.UrlEncode(VirtualHost)}/{name}?if-unused=true";
            HttpResponseMessage response = await HttpClient.DeleteAsync(url);
            if (response.StatusCode != HttpStatusCode.NoContent)
            {
                throw new Exception(response.ReasonPhrase); // Not Found
            }
        }

        #endregion

        #region "QUEUE"

        public async Task<List<QueueInfo>> GetQueues()
        {
            string url = $"/api/queues/{HttpUtility.UrlEncode(VirtualHost)}";
            HttpResponseMessage response = await HttpClient.GetAsync(url);
            Stream stream = await response.Content.ReadAsStreamAsync();
            List<QueueInfo> list = await JsonSerializer.DeserializeAsync<List<QueueInfo>>(stream);
            return list;
        }
        public async Task<QueueResponse> GetQueues(int page, int size, string regex)
        {
            if (size == 0 || size > 100)
            {
                size = 100;
            }
            string url = $"/api/queues/{HttpUtility.UrlEncode(VirtualHost)}?page={page}&page_size={size}&name={HttpUtility.UrlEncode(regex)}&use_regex=true";
            HttpResponseMessage response = await HttpClient.GetAsync(url);
            Stream stream = await response.Content.ReadAsStreamAsync();
            QueueResponse result = await JsonSerializer.DeserializeAsync<QueueResponse>(stream);
            return result;
        }
        public async Task<QueueInfo> GetQueue(string name)
        {
            string url = $"/api/queues/{HttpUtility.UrlEncode(VirtualHost)}?page=1&page_size=1&name={name}";
            HttpResponseMessage response = await HttpClient.GetAsync(url);
            Stream stream = await response.Content.ReadAsStreamAsync();
            QueueResponse queues = await JsonSerializer.DeserializeAsync<QueueResponse>(stream);
            return (queues == null || queues.Items == null || queues.Items.Count == 0)
                ? null
                : queues.Items[0];
        }
        public async Task CreateQueue(string name)
        {
            string url = $"/api/queues/{HttpUtility.UrlEncode(VirtualHost)}/{name}";
            QueueRequest queue = new QueueRequest()
            {
                Durable = true
            };
            HttpResponseMessage response = await HttpClient.PutAsJsonAsync(url, queue);
            if (response.StatusCode != HttpStatusCode.Created)
            {
                throw new Exception(response.ReasonPhrase); // No Content
            }
        }
        public async Task DeleteQueue(string name)
        {
            string url = $"/api/queues/{HttpUtility.UrlEncode(VirtualHost)}/{name}";
            HttpResponseMessage response = await HttpClient.DeleteAsync(url);
            if (response.StatusCode != HttpStatusCode.NoContent)
            {
                throw new Exception(response.ReasonPhrase); // Not Found
            }
        }
        public async Task SafeDeleteQueue(string name)
        {
            // When DELETEing a queue you can add the query string parameters if-empty=true and / or if-unused=true.
            // These prevent the delete from succeeding if the queue contains messages, or has consumers, respectively.

            string url = $"/api/queues/{HttpUtility.UrlEncode(VirtualHost)}/{name}?if-empty=true&if-unused=true";
            HttpResponseMessage response = await HttpClient.DeleteAsync(url);
            if (response.StatusCode != HttpStatusCode.NoContent)
            {
                throw new Exception(response.ReasonPhrase); // Not Found
            }
        }

        #endregion

        #region "BINDING"

        public async Task<List<BindingInfo>> GetBindings()
        {
            string url = $"/api/bindings/{HttpUtility.UrlEncode(VirtualHost)}";
            HttpResponseMessage response = await HttpClient.GetAsync(url);
            Stream stream = await response.Content.ReadAsStreamAsync();
            List<BindingInfo> list = await JsonSerializer.DeserializeAsync<List<BindingInfo>>(stream);
            return list;
        }
        public async Task<List<BindingInfo>> GetBindings(QueueInfo queue)
        {
            string url = $"/api/queues/{HttpUtility.UrlEncode(VirtualHost)}/{queue.Name}/bindings";
            HttpResponseMessage response = await HttpClient.GetAsync(url);
            Stream stream = await response.Content.ReadAsStreamAsync();
            List<BindingInfo> list = await JsonSerializer.DeserializeAsync<List<BindingInfo>>(stream);
            return list;
        }
        public async Task<List<BindingInfo>> GetBindings(ExchangeInfo exchange)
        {
            string url = $"/api/exchanges/{HttpUtility.UrlEncode(VirtualHost)}/{exchange.Name}/bindings/source";
            HttpResponseMessage response = await HttpClient.GetAsync(url);
            Stream stream = await response.Content.ReadAsStreamAsync();
            List<BindingInfo> list = await JsonSerializer.DeserializeAsync<List<BindingInfo>>(stream);
            return list;
        }
        public async Task CreateBinding(ExchangeInfo exchange, QueueInfo queue, string routingKey)
        {
            string url = $"/api/bindings/{HttpUtility.UrlEncode(VirtualHost)}/e/{exchange.Name}/q/{queue.Name}";
            BindingRequest binding = new BindingRequest()
            {
                RoutingKey = (routingKey == null ? string.Empty : routingKey)
            };
            HttpResponseMessage response = await HttpClient.PostAsJsonAsync(url, binding);
            if (response.StatusCode != HttpStatusCode.Created)
            {
                throw new Exception(response.ReasonPhrase); // No Content
            }
            else
            {
                Uri location = response.Headers.Location;
            }
        }
        public async Task CreateBinding(ExchangeInfo source, ExchangeInfo destination, string routingKey)
        {
            string url = $"/api/bindings/{HttpUtility.UrlEncode(VirtualHost)}/e/{source.Name}/e/{destination.Name}";
            BindingRequest binding = new BindingRequest()
            {
                RoutingKey = (routingKey == null ? string.Empty : routingKey)
            };
            HttpResponseMessage response = await HttpClient.PostAsJsonAsync(url, binding);
            if (response.StatusCode != HttpStatusCode.Created)
            {
                throw new Exception(response.ReasonPhrase); // No Content
            }
            else
            {
                Uri location = response.Headers.Location;
            }
        }
        public async Task DeleteBinding(BindingInfo binding)
        {
            string type = (binding.DestinationType == "queue" ? "q" : "e");
            string url = $"/api/bindings/{HttpUtility.UrlEncode(VirtualHost)}/e/{binding.Source}/{type}/{binding.Destination}/{binding.PropertiesKey}";
            HttpResponseMessage response = await HttpClient.DeleteAsync(url);
            if (response.StatusCode != HttpStatusCode.NoContent)
            {
                throw new Exception(response.ReasonPhrase); // Not Found
            }
        }

        #endregion
    }
}