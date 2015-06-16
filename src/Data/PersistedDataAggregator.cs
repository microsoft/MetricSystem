// ---------------------------------------------------------------------
// <copyright file="PersistedDataAggregator.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Threading.Tasks;

    using Microsoft.IO;
    using MetricSystem.Utilities;

    internal interface IPersistedDataAggregator : IDisposable
    {
        /// <summary>
        /// Timeout for queries.
        /// </summary>
        TimeSpan Timeout { get; set; }

        /// <summary>
        /// The dimension set in use for the aggregated data.
        /// </summary>
        DimensionSet DimensionSet { get; }

        /// <summary>
        /// The maximum fanout for queries (effectively the number of machines to query). If the list of desired
        /// sources is larger than max fanout the sources will be distributed into evenly-sized blocks of no more than
        /// MaxFanout count.
        /// </summary>
        int MaxFanout { get; set; }

        /// <summary>
        /// Name of the data to retrieve.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Start time of the query.
        /// </summary>
        DateTime StartTime { get; }

        /// <summary>
        /// End time of the query.
        /// </summary>
        DateTime EndTime { get; }

        /// <summary>
        /// Execute data aggregation.
        /// </summary>
        /// <returns>A task whose return code indicates whether at least some data was retrieved or not.</returns>
        Task<bool> Run();

        /// <summary>
        /// Writes aggregated data to a stream.
        /// </summary>
        /// <param name="destination">The destination stream. Not disposed.</param>
        /// <returns>True if data was available to be written.</returns>
        bool WriteToStream(Stream destination);
    }

    internal sealed class PersistedDataAggregator<TInternal> : IPersistedDataAggregator
        where TInternal : class, IInternalData, new()
    {
        private const int DefaultMaxFanout = 50;
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(1);
        private readonly RecyclableMemoryStreamManager memoryStreamManager;
        private readonly Random randomNumberGenerator = new Random();
        private KeyedDataStore<TInternal> data;
        private HttpClient httpClient;

        public PersistedDataAggregator(string name, IEnumerable<string> sources, DateTime startTime, DateTime endTime,
                                       RecyclableMemoryStreamManager memoryStreamManager)
            : this(name, null, sources, startTime, endTime, memoryStreamManager) { }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "HttpClient lives for object lifetime.")]
        public PersistedDataAggregator(string name, DimensionSet dimensionSet, IEnumerable<string> sources,
                                       DateTime startTime, DateTime endTime, RecyclableMemoryStreamManager streamManager)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("Data name is invalid", name);
            }
            this.Name = name;
            this.Port = Protocol.DefaultServerPort; // TODO: move to ServerInfo

            if (dimensionSet != null)
            {
                this.DimensionSet = new DimensionSet(dimensionSet);
            }

            this.Sources = new List<PersistedDataSource>();
            foreach (var machine in sources)
            {
                if (string.IsNullOrWhiteSpace(machine))
                {
                    throw new ArgumentException("Invalid source name supplied", "sources");
                }
                this.Sources.Add(new PersistedDataSource(machine, PersistedDataSourceStatus.Unknown));
            }

            if (this.Sources.Count == 0)
            {
                throw new ArgumentException("Must supply one or more source", "sources");
            }

            if (startTime >= endTime)
            {
                throw new ArgumentException("startTime must be less than endTime", "startTime");
            }

            if (streamManager == null)
            {
                throw new ArgumentNullException("streamManager");
            }

            this.StartTime = startTime;
            this.EndTime = endTime;
            this.memoryStreamManager = streamManager;

            this.httpClient = new HttpClient(
                new WebRequestHandler
                {
                    AllowAutoRedirect = false,
                    AllowPipelining = true,
                    AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip,
                });

            this.MaxFanout = DefaultMaxFanout;
        }

        /// <summary>
        /// List of sources used for data retrieval.
        /// </summary>
        public IList<PersistedDataSource> Sources { get; private set; }

        public DimensionSet DimensionSet { get; set; }

        public TimeSpan Timeout
        {
            get { return this.httpClient.Timeout; }
            set { this.httpClient.Timeout = value; }
        }

        public int MaxFanout { get; set; }

        public string Name { get; private set; }

        /// <summary>
        /// Port to query -- TODO: remove and migrate to ServerInfo. This is some hax for quick testing.
        /// </summary>
        public ushort Port { get; set; }

        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }

        public async Task<bool> Run()
        {
            Events.Write.BeginPersistedDataAggregation(this.Name, this.StartTime, this.EndTime, this.Sources);

            if (this.Timeout <= TimeSpan.Zero)
            {
                this.Timeout = DefaultTimeout;
            }

            var tasks = new List<Task<bool>>();

            // We'll query up to 'MaxFanout' "blocks" of servers for data. If we get less than that we'll query each
            // server individually. If our MaxFanout is exceeded we will create that many blocks and attempt to
            // populate them evenly with queryable servers.
            IList<PersistedDataSource>[] blocks;
            if (this.Sources.Count <= this.MaxFanout)
            {
                blocks = new IList<PersistedDataSource>[this.Sources.Count];
                for (var i = 0; i < blocks.Length; ++i)
                {
                    blocks[i] = new List<PersistedDataSource> {this.Sources[i]};
                }
            }
            else
            {
                blocks = new IList<PersistedDataSource>[this.MaxFanout];
                var sourcesPerBlock = this.Sources.Count / (float)this.MaxFanout;

                var added = 0;
                // Sort the list of sources to attempt pod affinity within individual blocks.
                foreach (var src in (from source in this.Sources orderby source.Name select source))
                {
                    var currentBlock = (int)Math.Floor(added / sourcesPerBlock);
                    if (blocks[currentBlock] == null)
                    {
                        blocks[currentBlock] = new List<PersistedDataSource>();
                    }
                    blocks[currentBlock].Add(src);

                    ++added;
                }
            }

            Parallel.ForEach(blocks,
                             block =>
                             {
                                 var task = this.QuerySources(block);
                                 lock (tasks)
                                 {
                                     tasks.Add(task);
                                 }
                             });

            await Task.WhenAll(tasks);

            var success = tasks.Any(task => task.Result);

            Events.Write.EndPersistedDataAggregation(success);
            return success;
        }

        /// <summary>
        /// Write aggregated data to a provided stream.
        /// </summary>
        /// <param name="destination">Destination stream.</param>
        /// <returns>True if data was successfully serialized.</returns>
        public bool WriteToStream(Stream destination)
        {
            if (this.data == null || (this.data.Count == 0 && !this.data.HasUnmergedData))
            {
                return false;
            }

            this.data.Merge();
            using (var writer = new PersistedDataWriter(destination, this.DimensionSet, this.memoryStreamManager))
            {
                writer.WriteData(this.Name, this.StartTime.ToUniversalTime(), this.EndTime.ToUniversalTime(),
                                 (uint)this.data.Count, this.Sources, this.data);
            }

            return true;
        }

        public void Dispose()
        {
            if (this.httpClient != null)
            {
                this.httpClient.Dispose();
                this.httpClient = null;
            }
            if (this.data != null)
            {
                this.data.Dispose();
            }
        }

        /// <summary>
        /// Detach the aggregated data from the aggregator.
        /// </summary>
        /// <returns>The aggregated data (null if it was already detached).</returns>
        public KeyedDataStore<TInternal> AcquireData()
        {
            var ret = this.data;
            this.data = null;
            return ret;
        }

        private string CreateRequestUri(string machineName)
        {
            // TODO: fix hardcoded port number...
            return string.Format("http://{0}:{1}/transfer{2}?start={3:o}&end={4:o}",
                                 machineName, this.Port, this.Name, this.StartTime, this.EndTime);
        }

        private async Task<bool> QuerySources(IList<PersistedDataSource> sources)
        {
            Events.Write.BeginQuerySources(sources);

            var success = false;
            var transferRequest =
                new TransferRequest
                {
                    DataType = PersistedDataProtocol.GetPersistedTypeCodeFromType(typeof(TInternal)),
                    Timeout = (this.Timeout.Seconds * 9) / 10, // 90% of timeout goes to the child
                    MaxFanout = this.MaxFanout,
                    Sources = new List<string>()
                };

            foreach (var source in sources)
            {
                source.Status = PersistedDataSourceStatus.Unknown;
                transferRequest.Sources.Add(source.Name);
            }

            var serverOffset = this.randomNumberGenerator.Next(sources.Count);
            var selectedSource = sources[serverOffset];

            var request = new HttpRequestMessage(HttpMethod.Post, this.CreateRequestUri(selectedSource.Name));
            using (var ms = (this.memoryStreamManager.GetStream()))
            using (var writeStream = new WriterStream(ms, this.memoryStreamManager))
            {
                var writer = writeStream.CreateCompactBinaryWriter();
                writer.Write(transferRequest);
                request.Content = new ByteArrayContent(ms.ToArray());
            }

            try
            {
                Events.Write.BeginSendSourceQuery(selectedSource, request.RequestUri);
                var response =
                    await this.httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
                Events.Write.EndSendSourceQuery(selectedSource, (int)response.StatusCode, response.ReasonPhrase);

                if (!response.IsSuccessStatusCode)
                {
                    switch (response.StatusCode)
                    {
                    case HttpStatusCode.NotFound:
                        foreach (var source in sources)
                        {
                            source.Status = PersistedDataSourceStatus.Unavailable;
                        }
                        return true;
                    default:
                        return false;
                    }
                }

                Events.Write.BeginReceiveSourceResponseBody(selectedSource);
                var length = response.Content.Headers.ContentLength ?? 0;
                using (var dataStream = this.memoryStreamManager.GetStream("PersistedDataAggregator/Http/Read",
                                                                           (int)length, true))
                using (var responseStream = await response.Content.ReadAsStreamAsync())
                {
                    responseStream.CopyTo(dataStream);
                    dataStream.Position = 0;
                    using (var dataReader =
                        new PersistedDataReader(dataStream, this.memoryStreamManager, this.DimensionSet))
                    {
                        Events.Write.EndReceiveSourceResponseBody(selectedSource, (int)dataStream.Length);

                        success = true;
                        if (dataStream.Length == 0)
                        {
                            Events.Write.EmptyResponseReceivedFromSource(selectedSource);
                            // Over the line. Mark it zero. Or just available, but empty (which is okay!)
                            foreach (var source in sources)
                            {
                                source.Status = PersistedDataSourceStatus.Available;
                            }
                        }
                        else
                        {
                            try
                            {
                                this.UnpackResponse(dataReader);
                            }
                            catch (PersistedDataException ex)
                            {
                                Events.Write.PersistedDataExceptionFromSource(selectedSource, ex);
                                success = false;
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                if (ex is HttpRequestException || ex is IOException || ex is WebException || ex is ObjectDisposedException)
                {
                    Events.Write.HttpExceptionFromSource(selectedSource, ex);
                }
                else
                {
                    throw;
                }
            }

            Events.Write.EndQuerySources(success, sources);
            return success;
        }

        private void UnpackResponse(PersistedDataReader dataReader)
        {
            while (dataReader.ReadDataHeader())
            {
                lock (this)
                {
                    if (this.DimensionSet == null)
                    {
                        this.DimensionSet = dataReader.DimensionSet;
                    }

                    var remoteData = dataReader.LoadData<TInternal>();
                    if (!remoteData.Validate())
                    {
                        remoteData.Dispose();
                        throw new PersistedDataException("Remote data is invalid.");
                    }

                    if (this.data == null && this.DimensionSet.Equals(dataReader.DimensionSet))
                    {
                        this.DimensionSet = dataReader.DimensionSet;
                        this.data = remoteData;
                    }
                    else
                    {
                        if (this.data == null)
                        {
                            this.data = new KeyedDataStore<TInternal>(this.DimensionSet, this.memoryStreamManager);
                        }

                        this.data.TakeData(remoteData);
                        remoteData.Dispose();
                    }
                }

                foreach (var responseSource in dataReader.Header.Sources)
                {
                    // NOTE: We use 'StartsWith' below because lots of data is currently marked with
                    // the non-FQDN machine name. We can change it to 'Equals' in the future.
                    var localSource =
                        this.Sources.FirstOrDefault(s =>
                                                    s.Name.StartsWith(responseSource.Name,
                                                                      StringComparison.OrdinalIgnoreCase));
                    if (localSource == null)
                    {
                        // XXX: here for testing because we have to query 'localhost' but won't ever get something good
                        // back. Yes, this is STUPID.
                        if (this.Sources.Count == 1 && this.Sources[0].Name == "localhost")
                        {
                            localSource = this.Sources[0];
                        }
                        else
                        {
                            throw new PersistedDataException(
                                string.Format("Source {0} returned by server is unknown", responseSource.Name));
                        }
                    }
                    localSource.Status = responseSource.Status;
                }
            }
        }
    }
}
