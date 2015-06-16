// ---------------------------------------------------------------------
// <copyright file="Write.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
