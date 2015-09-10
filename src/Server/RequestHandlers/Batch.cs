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

namespace MetricSystem.Server.RequestHandlers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;

    using MetricSystem.Data;
    using MetricSystem.Client;

    using Events = MetricSystem.Server.Events;

    /// <summary>
    /// Simple request handler for processing batch queries on the fly. Simply delegates to the CounterRequestHandler for the real work
    /// </summary>
    internal sealed class BatchQueryRequestHandler : RequestHandler
    {
        private readonly CountersRequestHandler internalRequestHandler;
        private readonly Server server;

        public BatchQueryRequestHandler(Server server)
        {
            if (server == null)
            {
                throw new ArgumentNullException("server");
            }

            this.server = server;
            this.internalRequestHandler = new CountersRequestHandler(this.server);
        }

        public override string Prefix
        {
            get { return RestCommands.BatchQueryCommand; }
        }

        public override async Task<Response> ProcessRequest(Request request)
        {
            // we only accept POST requests
            if (!request.HasInputBody)
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "No request body provided.");
            }

            // make sure there is no specific resource being queries and that query parameters are empty - 
            // we don't want the caller to assume that these values are being used when they are in fact discarded
            if (request.Path.Length > 0 || request.QueryParameters.Count > 0)
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "URL parameters are not supported.");
            }

            return await this.ProcessQuery(request);
        }

        /// <summary>
        /// Process a /batchQuery query sent with possible fanout
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        private async Task<Response> ProcessQuery(Request request)
        {
            var query = await request.ReadInputBody<BatchQueryRequest>();
            if (query == null || query.Queries.Count == 0)
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "Invalid or empty request body.");
            }

            try
            {
                Task<BatchQueryResponse> distributedResponse;
                // kick off the distributed request if needed
                if (query.Sources != null && query.Sources.Count > 0)
                {
                    distributedResponse = this.server.CreateQueryClient(query).BatchQuery(query);
                }
                else if (this.server.EnableQueryAggregation && this.server.ServerList != null && this.server.ServerList.Any())
                {
                    // if we are an aggregation server, fanout to all servers
                    var fanoutRequest = (BatchQueryRequest)query.Clone();
                    fanoutRequest.Sources = this.server.ServerList.Servers;

                    distributedResponse = this.server.CreateQueryClient(fanoutRequest).BatchQuery(fanoutRequest);
                }
                else
                {
                    distributedResponse = null;
                }

                var localResponses = this.ProcessBatchQueryRequest(query);

                if (distributedResponse == null)
                {
                    await localResponses;
                    return Response.Create(request, HttpStatusCode.OK, CreateQueryResponse(query, localResponses.Result));
                }

                await Task.WhenAll(new Task[] {distributedResponse, localResponses});

                var merger = new BatchResponseAggregator(query);
                merger.AddResponse(distributedResponse.Result);
                merger.AddResponse(CreateQueryResponse(query, localResponses.Result));

                return Response.Create(request, HttpStatusCode.OK, merger.GetResponse());
            }
            catch (ArgumentException ex)
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        /// <summary>
        /// Execute the N queries against the CounterQueryHandler. Aggregate the responses in a single response object
        /// </summary>
        private async Task<IEnumerable<CounterQueryResponse>> ProcessBatchQueryRequest(BatchQueryRequest request)
        {
            Events.Write.BeginProcessingQuery("BatchQuery", "batch");
            var tasks = new List<Task<CounterQueryResponse>>(request.Queries.Count);
            tasks.AddRange(request.Queries.Select(RunCounterQuery));

            await Task.WhenAll(tasks);
            Events.Write.EndProcessingQuery("BatchQuery", "batch", 200);

            return tasks.Select(t => t.Result);
        }

        private Task<CounterQueryResponse> RunCounterQuery(BatchCounterQuery query)
        {
            return
                this.taskRunner.RunAsync(
                                         () =>
                                         {
                                             var innerResponse =
                                                 this.internalRequestHandler.Query(query.CounterName, null,
                                                                                   new DimensionSpecification(
                                                                                       query.QueryParameters)).Result;
                                                 innerResponse.UserContext = query.UserContext;
                                                 return innerResponse;
                                         });
        }

        /// <summary>
        /// Helper to create a final response based on request config
        /// </summary>
        private BatchQueryResponse CreateQueryResponse(BatchQueryRequest request, IEnumerable<CounterQueryResponse> responses)
        {
            return new BatchQueryResponse
                   {
                       RequestDetails = request.IncludeRequestDiagnostics
                                            ? new List<RequestDetails>
                                              {
                                                  new RequestDetails
                                                  {
                                                      Server = this.server.ServerInfo,
                                                      HttpResponseCode = 200,
                                                      IsAggregator = request.Sources != null && request.Sources.Count > 0,
                                                      Status = RequestStatus.Success
                                                  }
                                              }
                                            : new List<RequestDetails>(),
                       Responses = responses.ToList()
                   };
        }
    }
}
