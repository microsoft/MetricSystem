namespace MetricSystem.Client
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;

    /// <summary>
    /// Object which aggregates (merges) responses from a fanout operation
    /// </summary>
    public class BatchResponseAggregator
    {
        // map counter key to a per-counter aggregator and bool indicating whether the final response should be merged
        private readonly Dictionary<string, Tuple<bool, CounterAggregator>> requestMapping = new Dictionary<string, Tuple<bool, CounterAggregator>>();
        private readonly List<RequestDetails> responseDetails = new List<RequestDetails>();

        /// <summary>
        /// Constructor that validates the query request and initializes internal state and fixes/cleans up the individual queries
        /// </summary>
        public BatchResponseAggregator(BatchQueryRequest request)
        {
            if (request == null)
            {
                throw new ArgumentNullException("request");
            }

            if (request.Queries == null || request.Queries.Count == 0)
            {
                throw new ArgumentException("No queries provided", "request");
            }

            foreach (var counterRequest in request.Queries)
            {
                if (string.IsNullOrEmpty(counterRequest.UserContext))
                {
                    counterRequest.UserContext = Guid.NewGuid().ToString("D");
                }
                else if (this.requestMapping.ContainsKey(counterRequest.UserContext))
                {
                    throw new ArgumentException("Duplicate user context in request");
                }

                var counterAggregator = new CounterAggregator(counterRequest.QueryParameters);
                var shouldMergeSamples = DistributedQueryClient.ShouldMergeTimeBuckets(counterRequest.QueryParameters);

                counterRequest.QueryParameters = counterAggregator.ApplyPercentileCalculationAggregation(counterRequest.QueryParameters);
                this.requestMapping.Add(counterRequest.UserContext, new Tuple<bool, CounterAggregator>(shouldMergeSamples, counterAggregator));
            }
        }

        /// <summary>
        /// Add a response from a single machine
        /// </summary>
        public void AddResponse(BatchQueryResponse response)
        {
            if (response == null)
            {
                return;
            }

            if (response.RequestDetails != null)
            {
                lock (this.responseDetails)
                {
                    this.responseDetails.AddRange(response.RequestDetails);
                }
            }

            if (response.Responses == null)
            {
                return;
            }

            foreach (var counterResponse in response.Responses)
            {
                // will not lock here. No need since the dict is readonly at this point
                Tuple<bool, CounterAggregator> data;
                if (!this.requestMapping.TryGetValue(counterResponse.UserContext, out data))
                {
                    Events.Write.UnknownCounterResponse(counterResponse.UserContext);
                    continue;
                }

                data.Item2.AddMachineResponse(counterResponse);
            }
        }

        /// <summary>
        /// Build a final response
        /// </summary>
        public BatchQueryResponse GetResponse()
        {
            return new BatchQueryResponse
                   {
                       RequestDetails = this.responseDetails,
                       Responses = this.requestMapping.Select(kvp =>
                                                              {
                                                                  var counterResponse = kvp.Value.Item2.GetResponse(kvp.Value.Item1);
                                                                  counterResponse.UserContext = kvp.Key;
                                                                  counterResponse.HttpResponseCode = counterResponse.Samples != null && counterResponse.Samples.Count > 0 ? (short)HttpStatusCode.OK : (short)HttpStatusCode.NotFound;
                                                                  return counterResponse;
                                                              }).ToList()
                   };
        }
    }
}
