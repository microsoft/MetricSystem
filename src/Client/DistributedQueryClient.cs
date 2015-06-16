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

namespace MetricSystem.Client
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.IO;
    using MetricSystem.Data;
    using MetricSystem.Utilities;

    /// <summary>
    /// Generic client used for querying metric system servers in a distributed (tiered) fashion. Each request will be fanned out to 
    /// N (configurable) sources to collect information from each chunk of sources. Each source will then use this client to repeat the operation
    /// and aggregate data for its chunk
    /// </summary>
    public class DistributedQueryClient
    {
        /// <summary>
        /// Mockable interface which abstracts HttpWebClient usage. 
        /// Unit tests implement this in another assembly (hence 'internal' access)
        /// </summary>
        internal static IHttpRequesterFactory RequesterFactory = new HttpRequesterFactory();

        /// <summary>
        /// Random number used to randomly select a leader in a query block
        /// </summary>
        private readonly ThreadLocal<Random> random = new ThreadLocal<Random>(() => new Random());

        public DistributedQueryClient(TimeSpan timeout)
            : this(timeout, null) { }

        public DistributedQueryClient(TimeSpan timeout, RecyclableMemoryStreamManager memoryStreamManager)
        {
            if (memoryStreamManager == null)
            {
                memoryStreamManager = new RecyclableMemoryStreamManager(1 << 17, 1 << 20, 1 << 27);
            }

            this.Timeout = timeout;
            this.MemoryStreamManager = memoryStreamManager;
        }

        /// <summary>
        /// MemoryStreamManager helping to manage resources for this client
        /// </summary>
        public RecyclableMemoryStreamManager MemoryStreamManager { get; private set; }

        /// <summary>
        /// Timeout used for all requests from this client
        /// </summary>
        public TimeSpan Timeout { get; private set; }

        /// <summary>
        /// Execute a counter query against a single server.
        /// </summary>
        /// <param name="counterName">Name of the counter to query.</param>
        /// <param name="server">Server to query.</param>
        /// <param name="queryParameters">Optional query parameters for the counter.</param>
        /// <returns>single response aggregated from all servers</returns>
        public async Task<CounterQueryResponse> CounterQuery(string counterName, ServerInfo server,
                                                             IDictionary<string, string> queryParameters = null)
        {
            if (server == null)
            {
                throw new ArgumentNullException("server");
            }

            var request = new TieredRequest
                          {
                              Sources = new List<ServerInfo> {server},
                          };
            return await this.CounterQuery(counterName, request, queryParameters,
                                           DimensionSet.FromQueryParameters(queryParameters));
        }
        /// <summary>
        /// Execute a distributed counter query based on the initial request
        /// </summary>
        /// <param name="counterName">Name of the counter to query.</param>
        /// <param name="request">Request with source list</param>
        /// <param name="queryParameters">Optional query parameters for the counter.</param>
        /// <returns>single response aggregated from all servers</returns>
        public async Task<CounterQueryResponse> CounterQuery(string counterName, TieredRequest request,
                                                             IDictionary<string, string> queryParameters = null)
        {
            return await this.CounterQuery(counterName, request, queryParameters,
                                           DimensionSet.FromQueryParameters(queryParameters));
        }

        /// <summary>
        /// Run a distributed (tiered) query based on the parameters in the original request. Split the sources into N blocks (N is determined
        /// by the MaxFanout setting). Client will select a (random) leader for each block and send the request downstream. Client will merge all responses
        /// and in the case of failure will include diagnostics for each server in the chunk
        /// </summary>
        /// <param name="counterName">Name of the counter to query.</param>
        /// <param name="request">Base request to be distributed into blocks.</param>
        /// <param name="queryParameters">Query parameters for the counter.</param>
        /// <param name="dimensionSet">DimensionSet used to quickly merge per-sample query dimensions.</param>
        /// <returns>Aggregated response optionally returning per-source diagnostics. </returns>
        public async Task<CounterQueryResponse> CounterQuery(string counterName, TieredRequest request,
                                                             IDictionary<string, string> queryParameters,
                                                             DimensionSet dimensionSet)
        {
            if (string.IsNullOrEmpty(counterName))
            {
                throw new ArgumentException("No counter name specified", "counterName");
            }

            if (request == null)
            {
                throw new ArgumentNullException("request");
            }

            if (dimensionSet == null)
            {
                throw new ArgumentNullException("dimensionSet");
            }

            var counterAggregator = new CounterAggregator(dimensionSet);
            // if the client requested a percentile and there are multiple sources we let the aggregator apply that filtering after data collection
            if (HasSources(request))
            {
                queryParameters = counterAggregator.ApplyPercentileCalculationAggregation(queryParameters);
            }

            var command = Protocol.BuildCounterRequestCommand(RestCommands.CounterQueryCommand, counterName, queryParameters);

            return await this.Execute(request, command, counterAggregator.AddMachineResponse,
                                      () => counterAggregator.GetResponse(ShouldMergeTimeBuckets(queryParameters)));
        }

        /// <summary>
        /// Perform a distributed batch query (proxies to the generic Execute method of this class)
        /// </summary>
        public async Task<BatchQueryResponse> BatchQuery(BatchQueryRequest request)
        {
            var aggregator = new BatchResponseAggregator(request);

            return await this.Execute(request, RestCommands.BatchQueryCommand,
                                      aggregator.AddResponse,
                                      aggregator.GetResponse);
        }

        /// <summary>
        /// Get counter info from a single machine.
        /// </summary>
        /// <param name="counterPattern">Pattern to match counter names against (glob style).</param>
        /// <param name="server">Server to query.</param>
        /// <param name="queryParameters">Optional parameters for the query.</param>
        /// <returns>A CounterInfoResponse object with all retrieved information.</returns>
        public async Task<CounterInfoResponse> CounterInfoQuery(string counterPattern, ServerInfo server,
                                                                IDictionary<string, string> queryParameters = null)
        {
            if (server == null)
            {
                throw new ArgumentNullException("server");
            }

            var request = new TieredRequest
                          {
                              Sources = new List<ServerInfo> {server},
                          };
            return await this.CounterInfoQuery(counterPattern, request, queryParameters);
        }

        /// <summary>
        /// Get counter info from a group of machines.
        /// </summary>
        /// <param name="counterPattern">Pattern to match counter names against (glob style).</param>
        /// <param name="request">Base request to be distributed into blocks.</param>
        /// <param name="queryParameters">Optional parameters for the query.</param>
        /// <returns>A CounterInfoResponse object with all retrieved information.</returns>
        public async Task<CounterInfoResponse> CounterInfoQuery(string counterPattern, TieredRequest request,
                                                                IDictionary<string, string> queryParameters = null)
        {
            if (string.IsNullOrEmpty(counterPattern))
            {
                throw new ArgumentException("No pattern provided", "counterPattern");
            }

            if (request == null)
            {
                throw new ArgumentNullException("request");
            }

            var agg = new CounterInfoSampleCombiner(request.IncludeRequestDiagnostics);
            var command = Protocol.BuildCounterRequestCommand(RestCommands.CounterInfoCommand, counterPattern, queryParameters);

            return await this.Execute(request, command, agg.AddSamples, agg.GetResponse); 
        }

        /// <summary>
        /// Run a distributed (tiered) query based on the parameters in the original request. Split the sources into N blocks (N is determined
        /// by the MaxFanout setting). Client will select a (random) leader for each block and send the request downstream. Client will merge all responses
        /// and in the case of failure will include diagnostics for each server in the chunk
        /// </summary>
        /// <param name="request">Base request to be distribted into blocks</param>
        /// <param name="command">Command Uri to POST the request to on each server</param>
        /// <param name="onBlockCompletedAction">Action to take as each block request completes. May be called multiple times in parallel so the caller is responsible for locking</param>
        /// <param name="getMergedResponse">Func to return the final response for the caller. Called one time at the end of all block queries</param>
        /// <returns>Aggregated response optionally returning per-source diagnostics. </returns>
        public async Task<TResponseType> Execute<TRequestType, TResponseType>(
            TRequestType request, string command,
            Action<TResponseType> onBlockCompletedAction,
            Func<TResponseType> getMergedResponse)
            where TRequestType : TieredRequest, new()
            where TResponseType : TieredResponse, new()
        {
            if (string.IsNullOrWhiteSpace(command))
            {
                throw new ArgumentException("Need a valid command to send to the remote servers");
            }

            if (request == null || request.Sources == null)
            {
                throw new ArgumentException("Invalid request. Must have sources to request");
            }

            if (onBlockCompletedAction == null)
            {
                throw new ArgumentNullException("onBlockCompletedAction");
            }

            if (getMergedResponse == null)
            {
                throw new ArgumentNullException("getMergedResponse");
            }

            // if there are no sources, there is nothing for us to do. 
            if (!HasSources(request))
            {
                return getMergedResponse();
            }

            Events.Write.BeginDistributedQuery(command);

            var tasks = new List<Task>();

            using (var httpClient = RequesterFactory.GetRequester())
            {
                httpClient.Timeout = this.Timeout;

                // divide up the sources into blocks            
                var blocks = SplitSources(request.Sources, request.MaxFanout);

                // query each block in parallel 
                Parallel.ForEach(blocks, block =>
                                         {
                                             var task = this.QueryBlock(httpClient, block, request, 
                                                                        command, onBlockCompletedAction);
                                             lock (tasks)
                                             {
                                                 tasks.Add(task);
                                             }
                                         });
                await Task.WhenAll(tasks).ConfigureAwait(false);

                Events.Write.CreateMergedResponse();

                var response = getMergedResponse();

                Events.Write.EndDistributedQuery(command);

                return response;
            }
        }

        /// <summary>
        /// Given a query request, parse whether or not the time-separated samples need to be merged
        /// based on the AggregateDimension value. Default is false
        /// </summary>
        public static bool ShouldMergeTimeBuckets(IDictionary<string, string> queryParameters)
        {
            string aggregateSamplesValue;
            bool result;

            return (queryParameters != null &&
                    queryParameters.TryGetValue(ReservedDimensions.AggregateSamplesDimension, out aggregateSamplesValue) &&
                    bool.TryParse(aggregateSamplesValue, out result) && result);
        }

        /// <summary>
        /// Send a single query for a block of sources (select a random leader). 
        /// </summary>
        /// <param name="httpClient">Client to use for this request</param>
        /// <param name="blockSources">List of all sources in this block</param>
        /// <param name="originalRequest">The original request body. This will be cloned and modified for this specific block</param>
        /// <param name="command">command to send the http POST request to</param>
        /// <param name="onBlockCompleted">Aggregator which handles merging responses</param>
        /// <returns>CounterQueryResponse continaning the aggregated response for all sources in this block</returns>
        private async Task QueryBlock<TRequestType, TResponseType>(IHttpRequester httpClient, List<ServerInfo> blockSources,
                                                                   TRequestType originalRequest, string command,
                                                                   Action<TResponseType> onBlockCompleted)
            where TRequestType : TieredRequest, new()
            where TResponseType : TieredResponse, new()
        {
            var response =
                await
                this.SendQuery<TRequestType, TResponseType>(httpClient, blockSources, originalRequest, command);

            Events.Write.BeginBlockMerge();
            onBlockCompleted(response);
            Events.Write.CompleteBlockMerge();
        }

        /// <summary>
        /// Send a single query for a block of sources (select a random leader) and record responses
        /// </summary>
        /// <param name="httpClient">Client to use for this request</param>
        /// <param name="blockSources">List of all sources in this block</param>
        /// <param name="originalRequest">The original request body. This will be cloned and modified for this specific block</param>
        /// <param name="command">command to send the http POST request to</param>
        private async Task<TResponseType> SendQuery<TRequestType, TResponseType>(IHttpRequester httpClient,
                                                                                 List<ServerInfo> blockSources,
                                                                                 TRequestType originalRequest,
                                                                                 string command)
            where TRequestType : TieredRequest, new()
            where TResponseType : TieredResponse, new()
        {
            // select a random leader to process this block and remove it from the block
            var leaderIndex = this.random.Value.Next(blockSources.Count);
            var leader = blockSources[leaderIndex];
            blockSources.RemoveAt(leaderIndex);

            HttpResponseMessage responseMessage = null;

            try
            {
                var uri = string.Format("http://{0}:{1}{2}", leader.Hostname, leader.Port, command);
                var requestMessage = new HttpRequestMessage(HttpMethod.Post, uri);
                requestMessage.Headers.Add(HttpRequestHeader.Accept.ToString(), Protocol.BondCompactBinaryMimeType);

                // RequestMessage is disposed in the finalize statement below. That disposes its .Content which disposes the stream it is given
                requestMessage.Content = new StreamContent(this.CreatePostBody(originalRequest, blockSources));

                Events.Write.SendingBlockQuery(leader, uri, blockSources, this.Timeout.TotalMilliseconds);
                responseMessage = await httpClient.StartRequest(requestMessage).ConfigureAwait(false);
                Events.Write.ReceivedResponseHeaders(leader);

                // regardless of getting back a 200 or failure response (e.g. 4xx, 5xx), we still 
                // **expect** diagnostic information in the payload. Try to read it. If parsing the response
                // fails, we will look at the response code later. 

                // read the response body into a stream
                var length = responseMessage.Content.Headers.ContentLength ?? 0;
                using (var dataStream = this.MemoryStreamManager.GetStream("QueryClient/Http/Read",
                                                                           (int)length, true))
                {
                    using (var responseStream = await responseMessage.Content.ReadAsStreamAsync().ConfigureAwait(false))
                    {
                        // WARNING: Do NOT use await CopyToAsync. There is a BCL bug where certain exceptions
                        // are not marshaled back to the correct task and the await is never finished. This 
                        // shows up as a deadlock. To work around this, we use the synchronous API and are sad about it.
                        responseStream.CopyTo(dataStream);
                        dataStream.Position = 0;
                    }

                    Events.Write.ReceivedResponseBody(leader, dataStream.Length);

                    using (var readerStream = ReaderStream.FromMemoryStreamBuffer(dataStream, this.MemoryStreamManager))
                    {
                        var reader = readerStream.CreateCompactBinaryReader();
                        var result = reader.Read<TResponseType>();

                        Events.Write.QueryFinished(leader,
                                                   responseMessage.StatusCode == HttpStatusCode.OK
                                                       ? RequestStatus.Success.ToString()
                                                       : RequestStatus.ServerFailureResponse.ToString(),
                                                   (int)responseMessage.StatusCode,
                                                   responseMessage.ReasonPhrase
                            );

                        return result;
                    }
                }
            }
            catch (Exception ex)
            {
                if (IsTimedOutException(ex))
                {
                    Events.Write.QueryTimedOut(leader);
                    return TimedOutResponse<TResponseType>(originalRequest.IncludeRequestDiagnostics, leader,
                                                           blockSources);
                }

                if (responseMessage != null && !responseMessage.IsSuccessStatusCode)
                {
                    Events.Write.ServerFailureResponse(leader, responseMessage);
                    return ServerFailureResponse<TResponseType>(originalRequest.IncludeRequestDiagnostics,
                                                                leader,
                                                                responseMessage, blockSources);
                }

                if (ex is WebException || ex is IOException || ex is SocketException || ex is ObjectDisposedException ||
                    ex is InvalidDataException || ex is HttpRequestException || ex is UriFormatException)
                {
                    Events.Write.RequestException(leader, ex);
                    return ExceptionResponse<TResponseType>(originalRequest.IncludeRequestDiagnostics, leader,
                                                            ex, blockSources);
                }

                // rethrow unknown exceptions
                Events.Write.FatalExceptionWhenGatheringData(ex.Message, ex.StackTrace);
                throw;
            }
            finally
            {
                if (responseMessage != null)
                {
                    responseMessage.Dispose();
                }
            }
        }

        /// <summary>
        /// Clone the original request and replace the fanout information to be specific to this block
        /// </summary>
        private MemoryStream CreatePostBody<TRequestType>(TRequestType originalRequest, List<ServerInfo> blockSources)
            where TRequestType : TieredRequest, new()
        {
            var newRequest = (TRequestType)originalRequest.Clone();
            newRequest.FanoutTimeoutInMilliseconds = (originalRequest.FanoutTimeoutInMilliseconds * 9) / 10;
            //each  block gets 90% of the remaining timeout (arbitrarily)
            newRequest.Sources = blockSources;

            var requestStream = this.MemoryStreamManager.GetStream();
            using (var writer = new WriterStream(requestStream, this.MemoryStreamManager))
            {
                writer.CreateCompactBinaryWriter().Write(newRequest);
                requestStream.Position = 0;

                return requestStream;
            }
        }

        /// <summary>
        /// Split sources into N blocks by grouped by name (since machine name is based on machine physical location).
        /// Note: This is stolen from the PersistedDataAggregator. This will be consolidated when this client becomes generalized and
        /// used for *all* tiered/distributed queries
        /// </summary>
        /// <param name="sources">The sources needing to be queried</param>
        /// <param name="maxFanout">The maximum degree of fanout acceptable (thus max number of blocks)</param>
        /// <returns>Array of Blocks</returns>
        public static List<ServerInfo>[] SplitSources(IList<ServerInfo> sources, long maxFanout)
        {
            // We'll query up to 'MaxFanout' "blocks" of servers for data. If we get less than that we'll query each
            // server individually. If our MaxFanout is exceeded we will create that many blocks and attempt to
            // populate them evenly with queryable servers.
            List<ServerInfo>[] blocks;
            if (sources.Count <= maxFanout)
            {
                blocks = new List<ServerInfo>[sources.Count];
                for (var i = 0; i < blocks.Length; ++i)
                {
                    blocks[i] = new List<ServerInfo> {sources[i]};
                }
            }
            else
            {
                blocks = new List<ServerInfo>[maxFanout];
                var sourcesPerBlock = sources.Count / (float)maxFanout;

                var added = 0;
                // Sort the list of sources to attempt pod affinity within individual blocks.
                foreach (var src in (from source in sources orderby source select source))
                {
                    var currentBlock = (int)Math.Floor(added / sourcesPerBlock);
                    if (blocks[currentBlock] == null)
                    {
                        blocks[currentBlock] = new List<ServerInfo>();
                    }
                    blocks[currentBlock].Add(src);

                    ++added;
                }
            }

            return blocks;
        }

        private static bool HasSources(TieredRequest request)
        {
            return request != null && request.Sources != null && request.Sources.Count > 0;
        }

        #region Generate Failure Responses
        private static TResponseType ServerFailureResponse<TResponseType>(bool includeDiagnostics, ServerInfo leader,
                                                                          HttpResponseMessage response,
                                                                          IEnumerable<ServerInfo> sources)
            where TResponseType : TieredResponse, new()
        {
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                return CreateFailureResponse<TResponseType>(includeDiagnostics,
                                                            leader, RequestStatus.ServerFailureResponse,
                                                            (short)response.StatusCode, response.ReasonPhrase,
                                                            sources, RequestStatus.ServerFailureResponse,
                                                            404);
            }
            return CreateFailureResponse<TResponseType>(includeDiagnostics,
                                                        leader, RequestStatus.ServerFailureResponse,
                                                        (short)response.StatusCode, response.ReasonPhrase,
                                                        sources, RequestStatus.FederationError,
                                                        0);
        }

        private static bool IsTimedOutException(Exception ex)
        {
            var webException = ex as WebException;
            return ex is OperationCanceledException ||
                   (webException != null && webException.Status == WebExceptionStatus.Timeout);
        }

        private static TResponseType TimedOutResponse<TResponseType>(bool includeDiagnostics, ServerInfo leader,
                                                                     IEnumerable<ServerInfo> sources)
            where TResponseType : TieredResponse, new()
        {
            return CreateFailureResponse<TResponseType>(includeDiagnostics,
                                                        leader, RequestStatus.TimedOut, 0, null,
                                                        sources, RequestStatus.FederationError,
                                                        0);
        }

        private static TResponseType ExceptionResponse<TResponseType>(bool includeDiagnostics, ServerInfo leader,
                                                                      Exception ex, IEnumerable<ServerInfo> sources)
            where TResponseType : TieredResponse, new()
        {
            string exMessage = ex == null
                                   ? string.Empty
                                   : string.Format("{0}: {1}{2}", ex.GetType(), ex.Message,
                                                   ex.InnerException != null
                                                       ? string.Format(" ({0}: {1})", ex.InnerException.GetType(),
                                                                       ex.InnerException.Message)
                                                       : string.Empty);

            return CreateFailureResponse<TResponseType>(includeDiagnostics,
                                                        leader, RequestStatus.RequestException, 0,
                                                        exMessage,
                                                        sources, RequestStatus.FederationError,
                                                        0);
        }

        private static TResponseType CreateFailureResponse<TResponseType>(bool includeDiagnostics, ServerInfo server,
                                                                          RequestStatus leaderStatus,
                                                                          short leaderHttpStatus,
                                                                          string leaderStatusDescription,
                                                                          IEnumerable<ServerInfo> otherSources,
                                                                          RequestStatus sourceStatus,
                                                                          short sourceHttpStatus)
            where TResponseType : TieredResponse, new()
        {
            if (!includeDiagnostics)
            {
                return new TResponseType();
            }

            var response = new TResponseType();

            // include the leader response
            response.RequestDetails.Add(new RequestDetails
                                        {
                                            Server = server,
                                            Status = leaderStatus,
                                            HttpResponseCode = leaderHttpStatus,
                                            IsAggregator = otherSources.Any(),
                                            StatusDescription = leaderStatusDescription ?? string.Empty
                                        });

            // add the fanned out responses
            foreach (var source in otherSources)
            {
                response.RequestDetails.Add(new RequestDetails
                                            {
                                                Server = source,
                                                HttpResponseCode = sourceHttpStatus,
                                                Status = sourceStatus
                                            });
            }

            return response;
        }
        #endregion
    }
}
