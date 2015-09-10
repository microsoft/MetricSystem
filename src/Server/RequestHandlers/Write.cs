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
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;

    using MetricSystem.Data;
    using MetricSystem.Utilities;

    public sealed class WriteRequestHandler : RequestHandler
    {
        private readonly Server server;

        public WriteRequestHandler(Server server)
        {
            if (server == null)
            {
                throw new ArgumentNullException("server");
            }

            this.server = server;
        }

        public override string Prefix
        {
            get { return RestCommands.CounterWriteCommand; }
        }

        public override async Task<Response> ProcessRequest(Request request)
        {
            if (!request.HasInputBody)
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "No input body provided for write.");
            }

            if (string.IsNullOrEmpty(request.Path))
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "Counter name must be specified.");
            }

            var counter = this.server.DataManager.GetCounter<Counter>(request.Path);
            if (counter == null)
            {
                return request.CreateErrorResponse(HttpStatusCode.NotFound, "Unknown counter name provided.");
            }

            var input = await request.ReadInputBody<CounterWriteRequest>();
            if (input == null)
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "Could not deserialize input.");
            }
            if (input.Writes.Count == 0)
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "No write operations were provided.");
            }

            // We want to do two passes here because if any of the operations are invalid we want to reject the entire
            // transaction.
            if (input.Writes.Any(operation => operation.Count < 1))
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest,
                                                   "Operation counts must be greater than zero.");
            }

            foreach (var operation in input.Writes)
            {
                RunOperation(counter, operation);
            }

            return new Response(request, HttpStatusCode.Accepted, "Accepted");
        }

        private static void RunOperation(Counter counter, CounterWriteOperation op)
        {
            var timestamp = op.Timestamp == CounterWriteOperation.TimestampNow
                                ? DateTime.UtcNow
                                : op.Timestamp.ToDateTime();

            var dims = new DimensionSpecification(op.DimensionValues);

            var hitCounter = counter as HitCounter;
            var histogramCounter = counter as HistogramCounter;
            if (hitCounter != null)
            {
                hitCounter.Increment(op.Value * op.Count, dims, timestamp);
            }
            else
            {
                // It would be nice to direct-inject multiple values at once, but the APIs don't currently
                // support this. It's a reasonable amount of work to fix this and unknown whether folks will use this
                // a lot at this time.
                for (var i = 0; i < op.Count; ++i)
                {
                    histogramCounter.AddValue(op.Value, dims, timestamp);
                }
            }
        }
    }
}
