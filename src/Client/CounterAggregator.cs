namespace MetricSystem.Client
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using MetricSystem;
    using MetricSystem.Data;
    using MetricSystem.Utilities;

    /// <summary>
    /// Helper object which will combine counter data samples by dimensions and time ranges into a final sample series
    /// </summary>
    public class CounterAggregator
    {
        // mapping of Unique-SampleDimensions (as Data.Key) -> List of combined samples
        private readonly Dictionary<Key, SortedList<TimeRange, SampleCombiner>> dataDictionary =
            new Dictionary<Key, SortedList<TimeRange, SampleCombiner>>(EqualityComparer<Key>.Default);

        private readonly List<RequestDetails> requestDetails = new List<RequestDetails>(0);

        private readonly DimensionSet dimensionSet;
        private bool shouldFilterToFinalPercentile = false;
        private double requestedPercentile;

        public List<RequestDetails> RequestDetails
        {
            get { return this.requestDetails; }
        }

        public CounterAggregator(DimensionSet dimensionSet)
        {
            if (dimensionSet == null)
            {
                throw new ArgumentNullException("dimensionSet");
            }
            this.dimensionSet = dimensionSet;
        }

        public CounterAggregator(IDictionary<string, string> queryParameters)
            : this(DimensionSet.FromQueryParameters(queryParameters))
        {
        }

        /// <summary>
        /// Ask the counter aggregator to do all percentile computation as opposed to any metric system server. This will 
        /// strip out the 'percentile=xxx' parameter in the request and return the filtered parameter list. If a 
        /// percentile request was present, this will apply the percentile calculation in the final calculation of this object 
        /// </summary>
        /// <param name="queryParameters">Parameters to filter</param>
        /// <returns>Filtered parameter set</returns>
        public IDictionary<string, string> ApplyPercentileCalculationAggregation(IDictionary<string, string> queryParameters)
        {
            if (queryParameters == null)
            {
                return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            }

            string percentileParameter;
            double percentileRequested;

            var dimensionSpec = new DimensionSpecification(queryParameters);

            if (!dimensionSpec.TryGetValue(ReservedDimensions.PercentileDimension, out percentileParameter) ||
                !double.TryParse(percentileParameter, out percentileRequested) ||
                percentileRequested < 0 || percentileRequested > 100)
            {
                this.shouldFilterToFinalPercentile = false;
                return queryParameters;
            }

            this.shouldFilterToFinalPercentile = true;
            this.requestedPercentile = percentileRequested;

            var retVal = new Dictionary<string, string>(dimensionSpec, StringComparer.OrdinalIgnoreCase);
            retVal.Remove(ReservedDimensions.PercentileDimension);

            return retVal;
        }

        /// <summary>
        /// Add all samples from this query response. Combine overlapping time buckets as they are encountered. 
        /// This method IS threadsafe against itself.
        /// </summary>
        /// <param name="response"></param>
        public void AddMachineResponse(CounterQueryResponse response)
        {
            if (response == null)
            {
                throw new ArgumentNullException("response");
            }

            if (response.RequestDetails != null)
            {
                lock (this.requestDetails)
                {
                    this.requestDetails.AddRange(response.RequestDetails);
                }
            }

            if (response.Samples == null)
            {
                return;
            }

            foreach (var sample in response.Samples)
            {
                var baseSample = sample;

                var hashKey = this.dimensionSet.CreateKey(sample.Dimensions);
                var sampleTimeRange = CreateTimeRange(baseSample);
                var rangesToRemove = new List<TimeRange>();
                SampleCombiner combiner = null;
                SortedList<TimeRange, SampleCombiner> aggregatedBuckets;

                // grab the appropriate bucket list
                lock (this.dataDictionary)
                {
                    if (!this.dataDictionary.TryGetValue(hashKey, out aggregatedBuckets))
                    {
                        aggregatedBuckets = new SortedList<TimeRange, SampleCombiner>();
                        this.dataDictionary.Add(hashKey, aggregatedBuckets);
                    }
                }

                lock (aggregatedBuckets)
                {
                    // The buckets are ordered by start time - thus it is safe to merge and continue to 
                    // walk forward as we cannot ever merge which requires a backwards reprocess
                    foreach (var bucket in aggregatedBuckets)
                    {
                        var existingRange = bucket.Key;

                        // did we get past the end of the range we are interested in? 
                        if (existingRange.Start > sampleTimeRange.End)
                        {
                            break;
                        }

                        if (existingRange.IntersectsWith(sampleTimeRange))
                        {
                            sampleTimeRange = TimeRange.Merge(sampleTimeRange, existingRange);
                            rangesToRemove.Add(bucket.Key);

                            // if this is the first merge, just add this sample
                            if (combiner == null)
                            {
                                combiner = bucket.Value;
                                combiner.AddSample(sample);
                                combiner.MachineCount += SampleCombiner.ExtractMachineCount(sample);
                            }
                            else
                            {
                                // this is a N-merge (N > 1), thus sample is already accounted for in the combiner. Merge the values
                                combiner.Merge(bucket.Value);
                            }
                        }
                    }

                    // if there was no merge, then create a new bucket with this sample
                    if (combiner == null)
                    {
                        combiner = new SampleCombiner(sample)
                                   {
                                       MachineCount = SampleCombiner.ExtractMachineCount(sample)
                                   };
                    }

                    // remove the merged items and add the new item
                    foreach (var range in rangesToRemove)
                    {
                        aggregatedBuckets.Remove(range);
                    }

                    aggregatedBuckets.Add(sampleTimeRange, combiner);
                }
            }
        }

        /// <summary>
        /// Create a final response out of the aggregated data
        /// </summary>
        public CounterQueryResponse GetResponse(bool mergeTimeBuckets)
        {
            return new CounterQueryResponse
                   {
                       RequestDetails = this.RequestDetails,
                       Samples = mergeTimeBuckets 
                        ? this.TimeMergedSamples.ToList() 
                        : this.Samples.ToList()
                   };
        }

        /// <summary>
        /// Aggregated Samples
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public IEnumerable<DataSample> Samples
        {
            get
            {
                return from aggregatedBuckets in this.dataDictionary.Values
                    from pair in aggregatedBuckets
                    select (this.shouldFilterToFinalPercentile) ? this.CalculatePercentile(pair.Value.Data): pair.Value.Data;
            }
        }

        /// <summary>
        /// Combine all sample buckets into one single combined bucket. 
        /// </summary>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public IEnumerable<DataSample> TimeMergedSamples
        {
            get
            {
                foreach (var bucketList in this.dataDictionary.Values)
                {
                    SampleCombiner superAggregator = null;

                    foreach (var combiner in bucketList.Values)
                    {
                        if (superAggregator == null)
                        {
                            superAggregator = new SampleCombiner(combiner.Data)
                                              {
                                                  MachineCount = combiner.MachineCount
                                              };
                        }
                        else
                        {
                            superAggregator.Merge(combiner);
                        }
                    }

                    if (superAggregator != null)
                    {
                        if (this.shouldFilterToFinalPercentile)
                        {
                            yield return this.CalculatePercentile(superAggregator.Data);
                        }
                        else
                        {
                            yield return superAggregator.Data;
                        }
                    }
                }
            }
        }

        private DataSample CalculatePercentile(DataSample dataSample)
        {
            if (dataSample == null || dataSample.SampleType != DataSampleType.Histogram)
            {
                return dataSample;
            }

            return new DataSample
                   {
                       Name = dataSample.Name,
                       Dimensions = new Dictionary<string, string>(dataSample.Dimensions) {{ReservedDimensions.PercentileDimension, this.requestedPercentile.ToString("##.###")}},
                       StartTime = dataSample.StartTime,
                       EndTime = dataSample.EndTime,
                       SampleCount = dataSample.SampleCount,
                       MachineCount = dataSample.MachineCount,
                       SampleType = DataSampleType.Percentile,
                       Percentile = this.requestedPercentile,
                       PercentileValue = dataSample.Histogram.GetValueAtPercentile(this.requestedPercentile)
                   };
        }

        private static TimeRange CreateTimeRange(DataSample sample)
        {
            return new TimeRange(sample.StartTime, sample.EndTime);
        }
    }

}
