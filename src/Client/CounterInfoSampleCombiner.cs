// ---------------------------------------------------------------------
// <copyright file="CounterInfoSampleCombiner.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Client
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Abstracts the details of combining CounterInfo samples. Handles aggregating known dimensions and determining
    /// earliest start and end times.
    /// </summary>
    public sealed class CounterInfoSampleCombiner
    {
        private readonly Dictionary<CounterInfo, CounterInfo> knownCounters = new Dictionary<CounterInfo, CounterInfo>();

        private readonly List<RequestDetails> details = new List<RequestDetails>();
        private readonly bool shouldAggregateDetails;

        public CounterInfoSampleCombiner()
            : this(false) { }

        public CounterInfoSampleCombiner(bool shouldAggregateDetails)
        {
            this.shouldAggregateDetails = shouldAggregateDetails;
        }

        public void AddSamples(CounterInfoResponse response)
        {
            lock (this.knownCounters)
            {
                foreach (var item in response.Counters)
                {
                    this.AddSampleData(item);
                }
            }

            if (response.RequestDetails != null && this.shouldAggregateDetails)
            {
                lock (this.details)
                {
                    this.details.AddRange(response.RequestDetails);
                }
            }
        }

        public CounterInfoResponse GetResponse()
        {
            var response = new CounterInfoResponse { Counters = new List<CounterInfo>(this.knownCounters.Values), };
            if (this.shouldAggregateDetails)
            {
                response.RequestDetails = new List<RequestDetails>(this.details);
            }

            return response;
        }

        public static void Merge(CounterInfoResponse aggregatedResponse, CounterInfoResponse localResponse)
        {
            if (localResponse != null && localResponse.Counters.Count > 0)
            {
                // We want a fast lookup dictionary here.
                var aggregatedCounters = aggregatedResponse.Counters.ToDictionary(counterInfo => counterInfo);

                foreach (var counterInfo in localResponse.Counters)
                {
                    CounterInfo aggregateCounterInfo;
                    if (aggregatedCounters.TryGetValue(counterInfo, out aggregateCounterInfo))
                    {
                        MergeSampleData(aggregateCounterInfo, counterInfo);
                    }
                    else
                    {
                        aggregatedResponse.Counters.Add(counterInfo);
                    }
                }
            }
        }

        private void AddSampleData(CounterInfo newSample)
        {
            CounterInfo knownCounter;
            if (this.knownCounters.TryGetValue(newSample, out knownCounter))
            {
                MergeSampleData(knownCounter, newSample);
            }
            else
            {
                this.knownCounters.Add(newSample, newSample);
            }
        }

        private static void MergeSampleData(CounterInfo original, CounterInfo newSample)
        {
            foreach (var dim in newSample.Dimensions)
            {
                if (!original.Dimensions.Any(d => d.Equals(dim, StringComparison.OrdinalIgnoreCase)))
                {
                    original.Dimensions.Add(dim);
                }

                if (original.StartTime > newSample.StartTime)
                {
                    original.StartTime = newSample.StartTime;
                }
                if (original.EndTime < newSample.EndTime)
                {
                    original.EndTime = newSample.EndTime;
                }
            }

            if (newSample.DimensionValues != null)
            {
                // Necessary because of some fun quackery in Bond.
                original.FixDimensionValuesCaseSensitivity();
                foreach (var kvp in newSample.DimensionValues)
                {
                    var dim = kvp.Key;
                    var newValues = kvp.Value;
                    original.AddDimensionValues(dim, newValues);
                }
            }
        }
    }
}
