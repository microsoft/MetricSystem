// The MIT License (MIT)
//
// Copyright (c) 2015 Microsoft
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

namespace MetricSystem.Server
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading.Tasks;

    using MetricSystem.Data;
    using MetricSystem.Client;
    using MetricSystem.Server.RequestHandlers;
    using MetricSystem.Utilities;

    /// <summary>
    /// An HTTP server to provide a basic query interface to MetricSystem data.
    /// </summary>
    [SuppressMessage("Microsoft.Naming", "CA1724:TypeNamesShouldNotMatchNamespaces")]
    public sealed class Server : IDisposable
    {
        private AggregationPoller aggregationPoller;

        private readonly int maxConcurrentQueriesPerTaskPool;

        private readonly Dictionary<string, RequestHandler> requestHandlers = new Dictionary<string, RequestHandler>();
        private RegistrationClient registrationClient;
        private HttpServer server;
        private SemaphoreTaskRunner taskRunner;
        private readonly MetricSystem.ServerInfo serverInfo = new MetricSystem.ServerInfo();

        public const int DefaultMinimumResponseSizeToCompress = 16384;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="hostname">Hostname to listen on ("+" to listen on all addresses).</param>
        /// <param name="port">Port number to listen on. If 0 a random available port will be selected.</param>
        /// <param name="dataManager">Data Manager to retrieve data from.</param>
        public Server(string hostname, ushort port, DataManager dataManager)
            : this(hostname, port, dataManager, null, 0) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="hostname">Hostname to listen on ("+" to listen on all addresses).</param>
        /// <param name="port">Port number to listen on. If 0 a random available port will be selected.</param>
        /// <param name="dataManager">Data Manager to retrieve data from.</param>
        /// <param name="requestHandlers">Additional request handlers to use.</param>
        /// <param name="maxConcurrentQueries">Maximum number of queries which can be processed in parallel</param>
        public Server(string hostname, ushort port, DataManager dataManager,
                      IList<RequestHandler> requestHandlers, int maxConcurrentQueries)
        {
            if (port == 0)
            {
               this.FindAvailablePort();
            }
            else
            {
                this.Port = port;
            }

            if (dataManager == null)
            {
                throw new ArgumentNullException("dataManager");
            }

            this.Hostname = Dns.GetHostEntry("localhost").HostName;
            this.DataManager = dataManager;

            // TODO: move to JSON config.
            this.ServerList = new ServerList {ExpirationTime = TimeSpan.FromMinutes(10)};
            this.MinimumResponseSizeToCompress = DefaultMinimumResponseSizeToCompress;

            if (maxConcurrentQueries <= 0)
            {
                maxConcurrentQueries = Environment.ProcessorCount;
            }
            this.maxConcurrentQueriesPerTaskPool = maxConcurrentQueries;
            this.taskRunner = new SemaphoreTaskRunner(maxConcurrentQueries);

            this.AddHandler(new PingRequestHandler());
            this.AddHandler(new UnknownRequestHandler());

            this.AddHandler(new BatchQueryRequestHandler(this));
            this.AddHandler(new CountersRequestHandler(this));
            this.AddHandler(new ListServersRequestHandler(this.ServerList));
            this.AddHandler(new RegisterRequestHandler(this.ServerList));
            this.AddHandler(new TransferRequestHandler(this.DataManager));
            this.AddHandler(new WriteRequestHandler(this));

            if (requestHandlers != null)
            {
                foreach (var handler in requestHandlers)
                {
                    this.AddHandler(handler);
                }
            }

            // do not use 'this.Hostname' because, if it was localhost, we do only want to listen on loopback, but
            // GetHostEntry above will transform that into our full computer name and not honor this. Basically this
            // is some goofy voodoo magic.
            this.server = new HttpServer(hostname, this.Port);
            this.server.RequestReceived += this.OnRequestReceived;
        }

        /// <summary>
        /// Copy of the current server information.
        /// </summary>
        public MetricSystem.ServerInfo ServerInfo
        {
            get { return (MetricSystem.ServerInfo)this.serverInfo.Clone(); }
        }

        /// <summary>
        /// FQDN of this server.
        /// </summary>
        public string Hostname
        {
            get { return this.serverInfo.Hostname; }
            private set { this.serverInfo.Hostname = value; }
        }

        /// <summary>
        /// Machine function (may be optionally set prior to start) indicating what this server does.
        /// </summary>
        public string MachineFunction
        {
            get { return this.serverInfo.MachineFunction; }
            set
            {
                value = value ?? string.Empty;
                this.serverInfo.MachineFunction = value;
            }
        }

        /// <summary>
        /// Datacenter (may be optionally set prior to start) this server resides in.
        /// </summary>
        public string Datacenter
        {
            get { return this.serverInfo.Datacenter; }
            set
            {
                value = value ?? string.Empty;
                this.serverInfo.Datacenter = value;
            }
        }

        /// <summary>
        /// Port this server is running on.
        /// </summary>
        public ushort Port
        {
            get { return this.serverInfo.Port; }
            private set { this.serverInfo.Port = value; }
        }

        /// <summary>
        /// Minimum size of a response before compression will be utilized.
        /// </summary>
        public long MinimumResponseSizeToCompress { get; set; }

        /// <summary>
        /// Types of aggregation offered by the server.
        /// </summary>
        private AggregationTypes aggregationTypes;
        public AggregationTypes AggregationTypes
        {
            get { return this.aggregationTypes; }
            set
            {
                if (this.server.Running)
                {
                    throw new InvalidOperationException("Cannot modify aggregation behavior after server is started.");
                }

                var preAggregate = (int)(value & AggregationTypes.PreAggregate) != 0;
                if (preAggregate)
                {
                    this.aggregationPoller = new AggregationPoller(this.ServerList, this.DataManager,
                                                                   TimeSpan.FromMinutes(1));
                }
                else if (this.aggregationPoller != null)
                {
                    this.aggregationPoller.Dispose();
                    this.aggregationPoller = null;
                }

                this.aggregationTypes = value;
            }
        }

        public bool EnableQueryAggregation
        {
            get { return (int)(this.AggregationTypes & AggregationTypes.QueryAggregate) != 0; }
        }

        public bool EnablePreAggregation
        {
            get { return (int)(this.AggregationTypes & AggregationTypes.PreAggregate) != 0; }
        }

        /// <summary>
        /// DataManager associated with the server.
        /// </summary>
        public DataManager DataManager { get; private set; }
        
        /// <summary>
        /// List of servers which have registered with us for downstream aggregation.
        /// </summary>
        internal ServerList ServerList { get; private set; }

        public void Dispose()
        {
            this.Stop();

            if (this.aggregationPoller != null)
            {
                this.aggregationPoller.Dispose();
                this.aggregationPoller = null;
            }

            if (this.server != null)
            {
                this.server.Dispose();
                this.server = null;
            }

            if (this.registrationClient != null)
            {
                this.registrationClient.Dispose();
                this.registrationClient = null;
            }

            foreach (var handler in this.requestHandlers.Values)
            {
                if (handler.taskRunner != this.taskRunner)
                {
                    handler.taskRunner.Dispose();
                    handler.taskRunner = null;
                }
            }

            if (this.ServerList != null)
            {
                this.ServerList.Dispose();
                this.ServerList = null;
            }

            if (this.taskRunner != null)
            {
                this.taskRunner.Dispose();
                this.taskRunner = null;
            }
        }

        private void OnRequestReceived(HttpServer server, HttpListenerContext context)
        {
            RequestHandler handler;
            var commandPath = UnknownRequestHandler.Path;

            var path = context.Request.Url.AbsolutePath;
            if (path.Length > 1)
            {
                var firstSlash = path.IndexOf('/', 1);
                commandPath = (firstSlash > 0 ? path.Substring(0, firstSlash) : path);
            }

            if (!this.requestHandlers.TryGetValue(commandPath, out handler))
            {
                handler = this.requestHandlers[UnknownRequestHandler.Path];
            }

            var request = new Request(context, this.DataManager.MemoryStreamManager, this, handler);
            if (handler.TaskPool == RequestHandler.TaskPoolDesired.None)
            {
                Task.Factory.StartNew(() => this.ExecuteHandler(request));
            }
            else
            {
                // The task runner deals with exceptions so there's no reason to wait here.
#pragma warning disable 4014
                handler.taskRunner.RunAsync(() => this.ExecuteHandler(request));
#pragma warning restore 4014
            }
        }

        private async Task ExecuteHandler(Request request)
        {
            Events.Write.BeginHandlingRequest(request.Handler.Prefix, request.Path, request.QueryParameters);
            try
            {
                var response = await request.Handler.ProcessRequest(request);

                Events.Write.BeginSendResponse(request.Handler.Prefix);
                await response.Send();
                Events.Write.CompleteSendResponse(request.Handler.Prefix);
            }
            catch (Exception ex)
            {
                if (ex is HttpListenerException || ex is InvalidOperationException)
                {
                    return;
                }

                Events.Write.RequestException(request, ex);
                throw;
            }
            finally
            {
                try
                {
                    // must close the response context but may get an exception if the response was disposed
                    // elsewhere (client terminated the connection and the socket was disposed)
                    request.Context.Response.Close();
                }
                catch (HttpListenerException) {}
                catch (ObjectDisposedException) {}
                
                Events.Write.EndHandlingRequest(request.Handler.Prefix);
            }
        }

        public void SetRegistrationDestination(string destinationHostname, ushort destinationPort, TimeSpan interval)
        {
            if (this.DataManager == null)
            {
                throw new InvalidOperationException("Cannot enable registration without data.");
            }

            if (this.registrationClient != null)
            {
                this.registrationClient.Dispose();
            }

            this.registrationClient = new RegistrationClient(destinationHostname, destinationPort,
                                                             this.Hostname, this.Port, this.MachineFunction,
                                                             this.Datacenter, interval, this.DataManager);

            if (this.server.Running)
            {
                this.registrationClient.Start();
            }
        }

        public void Start()
        {
            this.server.Start();
            if (this.registrationClient != null)
            {
                this.registrationClient.Start();
            }

            if (this.aggregationPoller != null)
            {
                this.aggregationPoller.Start();
            }
        }

        public void Stop()
        {
            this.server.Stop();
        }

        internal DistributedQueryClient CreateQueryClient(TieredRequest request)
        {
            return new DistributedQueryClient(TimeSpan.FromMilliseconds(request.FanoutTimeoutInMilliseconds),
                this.DataManager.MemoryStreamManager);
        }

        private void FindAvailablePort()
        {
            var socket = new TcpListener(IPAddress.Loopback, 0);
            socket.Start();
            this.Port = (ushort)((IPEndPoint)socket.LocalEndpoint).Port;
            socket.Stop();
        }

        private void AddHandler(RequestHandler handler)
        {
            if (this.requestHandlers.ContainsKey(handler.Prefix))
            {
                throw new ArgumentException("Request handler " + handler.Prefix + " already exists.",
                                            "handler");
            }

            this.requestHandlers.Add(handler.Prefix, handler);

            if (handler.TaskPool == RequestHandler.TaskPoolDesired.Individual)
            {
                handler.taskRunner = new SemaphoreTaskRunner(this.maxConcurrentQueriesPerTaskPool);
            }
            else
            {
                handler.taskRunner = this.taskRunner;
            }
        }
    }
}
