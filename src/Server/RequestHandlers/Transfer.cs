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
    using System.Net;
    using System.Threading.Tasks;

    using MetricSystem.Data;

    internal sealed class TransferRequestHandler : RequestHandler
    {
        public const string CommandPrefix = "/transfer";
        private const string ResponseType = "bond/simple";
        private readonly DataManager dataManager;

        public TransferRequestHandler(DataManager dataManager)
        {
            if (dataManager == null)
            {
                throw new ArgumentNullException("dataManager");
            }

            this.dataManager = dataManager;
        }

        public override string Prefix
        {
            get { return CommandPrefix; }
        }

        public override async Task<Response> ProcessRequest(Request request)
        {
            var counterName = request.Path;
            if (string.IsNullOrEmpty(counterName))
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "Path or command is invalid.");
            }

            if (request.QueryParameters.Count != 2
                || !request.QueryParameters.ContainsKey(ReservedDimensions.StartTimeDimension)
                || !request.QueryParameters.ContainsKey(ReservedDimensions.EndTimeDimension))
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest,
                                                   "Invalid query parameters. Must specify only start and end time.");
            }

            DateTime start, end;
            if (!DateTime.TryParse(request.QueryParameters[ReservedDimensions.StartTimeDimension], out start))
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "Invalid start time.");
            }
            if (!DateTime.TryParse(request.QueryParameters[ReservedDimensions.EndTimeDimension], out end))
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "Invalid end time.");
            }

            var message = new TransferRequest();
            if (request.HasInputBody)
            {
                message = await request.ReadInputBody<TransferRequest>();
            }

            var ms = request.GetStream();

            // This is a tiered aggregation request, handle it separately.
            if (message.Sources != null && message.Sources.Count > 1)
            {
                try
                {
                    using (var aggregator =
                        PersistedDataProtocol.CreateAggregatorForSampleType(message.DataType, counterName,
                                                                            message.Sources, start, end,
                                                                            dataManager.MemoryStreamManager))
                    {
                        if (message.Timeout > 0)
                        {
                            aggregator.Timeout = TimeSpan.FromSeconds(message.Timeout);
                        }
                        if (message.MaxFanout > 0)
                        {
                            aggregator.MaxFanout = (int)message.MaxFanout;
                        }

                        // This shouldn't happen, given that one of the servers we want to talk to is.. us.
                        if (!await aggregator.Run())
                        {
                            ms.Dispose();
                            return request.CreateErrorResponse(HttpStatusCode.InternalServerError,
                                                               "All child requests failed.");
                        }
                        if (!aggregator.WriteToStream(ms))
                        {
                            // TODO: If we have no results but none of our queries failed we can definitively 404,
                            //       for now lazy.
                            ms.Dispose();
                            return request.CreateErrorResponse(HttpStatusCode.InternalServerError,
                                                               "No cntent matched.");
                        }
                    }
                }
                catch (ArgumentException)
                {
                    ms.Dispose();
                    return request.CreateErrorResponse(HttpStatusCode.BadRequest, "Request parameters are invalid.");
                }
            }
            else
            {
                var counter = this.dataManager.GetCounter<Counter>(counterName);
                if (counter == null || !counter.SerializeData(start, end, ms))
                {
                    ms.Dispose();
                    return request.CreateErrorResponse(HttpStatusCode.NotFound, "The requested data is not available.");
                }
            }

            // after writing the response the server will handle disposing the stream for us.
            return new Response(request, HttpStatusCode.OK, ms) {ContentType = ResponseType};
        }
    }
}
