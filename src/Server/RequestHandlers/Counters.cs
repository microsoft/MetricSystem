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
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;

    using MetricSystem.Data;
    using MetricSystem.Client;
    using MetricSystem.Utilities;

    using Events = MetricSystem.Server.Events;

    internal sealed class CountersRequestHandler : RequestHandler
    {
        private readonly Server server;

        public CountersRequestHandler(Server server)
        {
            if (server == null)
            {
                throw new ArgumentNullException("server");
            }

            this.server = server;
        }

        public override string Prefix
        {
            get { return RestCommands.CounterRequestCommand; }
        }

        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public override async Task<Response> ProcessRequest(Request request)
        {
            var fanoutRequest = request.HasInputBody ? await request.ReadInputBody<TieredRequest>() : null;
            if (request.HasInputBody && fanoutRequest == null)
            {
                // This indicates failed deserialization
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "Could not read input body");
            }

            // aggregation servers should fanout automagically. we honor filters for machine function and datacenter.
            if ((fanoutRequest == null || fanoutRequest.Sources == null || fanoutRequest.Sources.Count == 0) && this.server.EnableQueryAggregation)
            {
                if (fanoutRequest == null)
                {
                    fanoutRequest = new TieredRequest();
                }

                fanoutRequest.Sources = this.SelectSourceServers(request.QueryParameters).ToList();
            }

            var queryParameters = new DimensionSpecification(request.QueryParameters);
            var endOfName = request.Path.LastIndexOf('/');
            if (endOfName < 0)
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "No command provided.");
            }

            var counterName = request.Path.Substring(0, endOfName);
            if (string.IsNullOrEmpty(counterName))
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "No counter pattern provided.");
            }

            var commandName = request.Path.Substring(endOfName + 1);

            if (RestCommands.CounterInfoCommand.Equals(commandName, StringComparison.OrdinalIgnoreCase))
            {
                return await this.Info(request, fanoutRequest, counterName, queryParameters);
            }

            if (RestCommands.CounterQueryCommand.Equals(commandName, StringComparison.OrdinalIgnoreCase))
            {
                var response = await this.Query(counterName, fanoutRequest, queryParameters);
                return Response.Create(request, (HttpStatusCode)response.HttpResponseCode, response);
            }

            return request.CreateErrorResponse(HttpStatusCode.BadRequest, "Unknown command: " + commandName);
        }

        private IEnumerable<MetricSystem.ServerInfo> SelectSourceServers(IDictionary<string, string> queryParameters)
        {
            string machineFunctionPattern;
            if (!queryParameters.TryGetValue(ReservedDimensions.MachineFunctionDimension, out machineFunctionPattern))
            {
                machineFunctionPattern = "*";
            }
            string datacenterPattern;
            if (!queryParameters.TryGetValue(ReservedDimensions.DatacenterDimension, out datacenterPattern))
            {
                datacenterPattern = "*";
            }

            return from server in this.server.ServerList.Servers
                   where
                       server.MachineFunction.MatchGlob(machineFunctionPattern) &&
                       server.Datacenter.MatchGlob(datacenterPattern)
                   select server;
        }

        #region query command
        internal async Task<CounterQueryResponse> Query(string counterName, TieredRequest fanoutRequest,
                                                        DimensionSpecification queryParameters)
        {
            Task<CounterQueryResponse> distributedRequest;

            // if fanout is required, start it here (don't block)
            if (fanoutRequest != null && fanoutRequest.Sources != null && fanoutRequest.Sources.Count > 0)
            {
                distributedRequest = this.server.CreateQueryClient(fanoutRequest)
                                         .CounterQuery(counterName, fanoutRequest, queryParameters,
                                                       DimensionSet.FromQueryParameters(queryParameters));
            }
            else
            {
                distributedRequest = Task.FromResult<CounterQueryResponse>(null);
            }

            // get the local data (in parallel to the async distributed request running, if necessary)
            var localResponse = new CounterQueryResponse();
            try
            {
                var counter = this.server.DataManager.GetCounter<Counter>(counterName);
                if (counter == null)
                {
                    localResponse.HttpResponseCode = (int)HttpStatusCode.NotFound;
                    localResponse.ErrorMessage = "Counter not found.";
                }
                else
                {
                    Events.Write.BeginProcessingQuery(RestCommands.CounterQueryCommand, counterName);
                    localResponse.Samples = counter.Query(queryParameters).ToList();

                    if (localResponse.Samples.Count > 0)
                    {
                        Events.Write.EndProcessingQuery("CounterQuery", counter.Name, 200);
                        localResponse.HttpResponseCode = (int)HttpStatusCode.OK;
                    }
                    else
                    {
                        Events.Write.EndProcessingQuery("CounterQuery", counter.Name, 404);
                        localResponse.HttpResponseCode = (int)HttpStatusCode.NotFound;
                        localResponse.ErrorMessage = "No data matched query.";
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex is FormatException || ex is ArgumentOutOfRangeException || ex is ArgumentException
                    || ex is KeyNotFoundException || ex is NotSupportedException)
                {
                     localResponse.HttpResponseCode = (int)HttpStatusCode.BadRequest;
                     localResponse.ErrorMessage = ex.Message;
                }
                else
                {
                    throw;
                }
            }

            // wait for the distributed request to finish
            var distributedResponse = await distributedRequest;

            if (fanoutRequest != null && fanoutRequest.IncludeRequestDiagnostics && localResponse.RequestDetails != null)
            {
                localResponse.RequestDetails.Add(new RequestDetails
                {
                    Server = new MetricSystem.ServerInfo
                             {
                                 Hostname = this.server.Hostname,
                                 Port = this.server.Port,
                                 Datacenter = this.server.Datacenter,
                                 MachineFunction = this.server.MachineFunction,
                             },
                    HttpResponseCode = localResponse.HttpResponseCode,
                    StatusDescription = localResponse.ErrorMessage ?? string.Empty,
                    Status = localResponse.HttpResponseCode == (short)HttpStatusCode.OK ? RequestStatus.Success : RequestStatus.ServerFailureResponse,
                    IsAggregator = distributedResponse != null
                });
            }

            var response = MergeResponses(queryParameters, localResponse,
                                          DistributedQueryClient.ShouldMergeTimeBuckets(queryParameters),
                                          distributedResponse);

            return response;
        }

        /// <summary>
        /// Merge the local Query request and the distributed tiered response into a single response
        /// </summary>
        private static CounterQueryResponse MergeResponses(IDictionary<string, string> queryParameters,
                                                           CounterQueryResponse localResponse, bool mergeTimeBuckets,
                                                           CounterQueryResponse distributedResponse)
        {
            // no distributed response, just use the local one
            if (distributedResponse == null)
            {
                return localResponse;
            }

            var sampleMerger = new CounterAggregator(queryParameters ?? new Dictionary<string, string>(0));
            sampleMerger.AddMachineResponse(localResponse);
            sampleMerger.AddMachineResponse(distributedResponse);

            // response code from this server is:
            //  200: *Anyone* has data
            //  40X: *Everyone failed to get data, but everyone has the same response code
            //  409: Mixed error codes ("Conflict" used liberally)
            HttpStatusCode responseCode;
            if ((localResponse.Samples != null && localResponse.Samples.Count > 0) ||
                (distributedResponse.Samples != null && distributedResponse.Samples.Count > 0))
            {
                responseCode = HttpStatusCode.OK;
            }
            else
            {
                responseCode = distributedResponse.RequestDetails != null &&
                               distributedResponse.RequestDetails.Count > 0 &&
                               distributedResponse.RequestDetails.All(d => d.HttpResponseCode == (int)localResponse.HttpResponseCode)
                                   ? (HttpStatusCode)localResponse.HttpResponseCode
                                   : HttpStatusCode.Conflict;
            }

            var mergedResponse = sampleMerger.GetResponse(mergeTimeBuckets);
            mergedResponse.HttpResponseCode = (short)responseCode;
            return mergedResponse;
        }

        #endregion

        #region info command
        private async Task<Response> Info(Request request, TieredRequest fanoutRequest, string counterPattern,
                                          DimensionSpecification queryParameters)
        {
            var localResponseData = new CounterInfoResponse();
            foreach (var c in this.server.DataManager.Counters)
            {
                if (c.Name.MatchGlob(counterPattern))
                {
                    localResponseData.Counters.Add(BuildCounterInfo(c, queryParameters));
                }
            }

            // If there's no lower tier we can bounce out now.
            if (fanoutRequest == null)
            {
                return localResponseData.Counters.Count == 0
                           ? request.CreateErrorResponse(HttpStatusCode.NotFound, "No matching counters are defined.")
                           : Response.Create(request, HttpStatusCode.OK, localResponseData);
            }

            var distributedResponse =
                await
                this.server.CreateQueryClient(fanoutRequest)
                    .CounterInfoQuery(counterPattern, fanoutRequest, queryParameters);

            CounterInfoSampleCombiner.Merge(distributedResponse, localResponseData);
            // add details from this server if needed
            if (fanoutRequest.IncludeRequestDiagnostics && distributedResponse.RequestDetails != null)
            {
                distributedResponse.RequestDetails.Add(new RequestDetails
                                                       {
                                                           Server = this.server.ServerInfo,
                                                           IsAggregator = true,
                                                           HttpResponseCode = (localResponseData.Counters.Count > 0
                                                                                   ? (short)HttpStatusCode.OK
                                                                                   : (short)HttpStatusCode.NotFound),
                                                           Status = RequestStatus.Success,
                                                       });
            }

            return Response.Create(request, HttpStatusCode.OK, distributedResponse);
        }

        private static readonly string MinimumStartTime = DateTime.MinValue.ToString(Protocol.TimestampStringFormat);
        private static readonly string MaximumEndTime = DateTime.MaxValue.ToString(Protocol.TimestampStringFormat);
        private static CounterInfo BuildCounterInfo(Counter counter, DimensionSpecification queryParameters)
        {
            var counterInfo = new CounterInfo
                              {
                                  Name = counter.Name,
                                  Type = counter.Type,
                                  StartTime = counter.StartTime.ToMillisecondTimestamp(),
                                  EndTime = counter.EndTime.ToMillisecondTimestamp(),
                                  Dimensions = counter.Dimensions.ToList(),
                                  DimensionValues = null, // null this out by default to avoid response bloat.
                              };

            // Queries for dimension values will come with a 'dimension=pattern' query parameter.
            // Dimension values can be further filtered with '<dimensionName>=pattern'
            string dimensionPattern;
            if (queryParameters.TryGetValue(ReservedDimensions.DimensionDimension, out dimensionPattern))
            {
                counterInfo.DimensionValues = new Dictionary<string, ISet<string>>();

                // We want to be able to filter dimension values by time (and only time)
                var dimensionQuery = new DimensionSpecification();
                string timeValue;
                if (!queryParameters.TryGetValue(ReservedDimensions.StartTimeDimension, out timeValue))
                {
                    timeValue = MinimumStartTime;
                }
                dimensionQuery[ReservedDimensions.StartTimeDimension] = timeValue;
                if (!queryParameters.TryGetValue(ReservedDimensions.EndTimeDimension, out timeValue))
                {
                    timeValue = MaximumEndTime;
                }
                dimensionQuery[ReservedDimensions.EndTimeDimension] = timeValue;

                foreach (var dim in counter.Dimensions.Where(d => d.MatchGlob(dimensionPattern)))
                {
                    string filterPattern;
                    if (queryParameters.TryGetValue(dimensionPattern, out filterPattern))
                    {
                        
                        counterInfo.AddDimensionValues(dim,
                                                       counter.GetDimensionValues(dim, dimensionQuery)
                                                              .Where(dimensionValue =>
                                                                     dimensionValue.MatchGlob(filterPattern)));
                    }
                    else
                    {
                        counterInfo.AddDimensionValues(dim, counter.GetDimensionValues(dim, dimensionQuery));
                    }
                }
            }

            return counterInfo;
        }
        #endregion
    }
}
