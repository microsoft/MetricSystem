// ---------------------------------------------------------------------
// <copyright file="DataSet.cs" company="Microsoft">
//       Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;

    using MetricSystem.Utilities;

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1506:AvoidExcessiveClassCoupling")]
    internal sealed class DataSet<TInternal>
        : IDataSet
        where TInternal : class, IInternalData, new()
    {
        public string Name { get; private set; }

        public DimensionSet DimensionSet { get; private set; }

        public event DataSealedHandler OnDataSealed;

        public DateTimeOffset StartTime
        {
            get
            {
                using (SharedLock.OpenShared(this.dataLock))
                {
                    for (int i = this.data.Count - 1; i >= 0; --i)
                    {
                        if (this.data.Values[i].Sealed)
                        {
                            return new DateTimeOffset(this.data.Values[i].StartTime);
                        }
                    }

                    return DateTimeOffset.MaxValue;
                }
            }
        }

        public DateTimeOffset EndTime
        {
            get
            {
                using (SharedLock.OpenShared(this.dataLock))
                {
                    for (int i = 0; i < this.data.Count; ++i)
                    {
                        if (this.data.Values[i].Sealed)
                        {
                            return new DateTimeOffset(this.data.Values[i].EndTime);
                        }
                    }

                    return DateTimeOffset.MinValue;
                }
            }
        }

        private readonly SortedList<DateTime, DataBucket<TInternal>> data;
        private readonly string storagePath;
        private readonly ISharedDataSetProperties properties;
        
        private ReaderWriterLockSlim dataLock = new ReaderWriterLockSlim();
        
        // variable used in Interlocked.CompareExchange to ensure that only one compaction happens at a time 
        private int compactionInProgress = 0;

        private DateTime latestStartTime = DateTime.MinValue;
        private DateTime earliestUnsealedBucketTime = DateTime.MinValue;

        public DataSet(string name, string storagePath, DimensionSet dimensionSet, ISharedDataSetProperties properties)
        {
            this.Name = name;
            this.storagePath = storagePath;
            this.DimensionSet = dimensionSet;
            this.properties = properties;

            this.data = new SortedList<DateTime, DataBucket<TInternal>>(
                Comparer<DateTime>.Create((x, y) => y.CompareTo(x)));
        }

        public override string ToString()
        {
            return string.Format("{0}: {1} buckets {2}", this.Name, this.data.Count, this.storagePath);
        }

        public void AddValue(long value, DimensionSpecification dims, DateTime timestamp)
        {
            timestamp = timestamp.ToUniversalTime();
            DataBucket<TInternal> bucket;

            using (SharedLock.OpenExclusive(this.dataLock))
            {
                bucket = this.GetOrCreateDataBucket(timestamp, true);

                if (bucket == null)
                {
                    Events.Write.RejectedAttemptToWriteAncientData(this.Name, this.earliestUnsealedBucketTime,
                                                                   timestamp);
                    return;
                }
            }

            bucket.AddValue(dims, value);
        }

        internal DataBucket<TInternal> GetDataBucket(DateTime timestamp)
        {
            for (var i = 0; i < this.data.Count; ++i)
            {
                var bucket = this.data.Values[i];
                if (bucket.HasDataForTimestamp(timestamp))
                {
                    return bucket;
                }

                if (bucket.StartTime < timestamp)
                {
                    break;
                }
            }

            return null;
        }

        private void AddBucket(DataBucket<TInternal> newBucket)
        {
            this.data.Add(newBucket.StartTime, newBucket);
            if (newBucket.StartTime > this.latestStartTime)
            {
                this.latestStartTime = newBucket.StartTime;
            }

            // Every time we add a new bucket we re-scan our buckets to see if some data should be sealed or deleted
            // the current bucket being added may (generally isn't, but may) not be for the newest time.
            // Because we expect a low overall bucket count (topping out in the thousands) with relatively infrequent 
            // bucket addition, this scan is deemed reasonable. Additionally we make sure to stop the scan once 
            // no more buckets could be impacted. The expectation is that we will generally scan only the last bucket 
            // before bailing out after some relatively lightweight arithmetic.

            var maxScanTime = DateTime.MinValue;
            var maxBucketAge = DateTime.MinValue;
            var maxUnsealedAge = DateTime.MinValue;

            if (this.properties.MaximumDataAge > TimeSpan.Zero)
            {
                maxBucketAge = this.data.Values[0].StartTime - this.properties.MaximumDataAge;
                maxScanTime = maxBucketAge;
            }
            if (this.properties.SealTime > TimeSpan.Zero)
            {
                maxUnsealedAge = this.data.Values[0].StartTime - this.properties.SealTime;
                maxScanTime = new DateTime(Math.Max(maxScanTime.Ticks, maxUnsealedAge.Ticks), DateTimeKind.Utc);
            }
            this.earliestUnsealedBucketTime = maxScanTime;

            if (maxScanTime == DateTime.MinValue)
            {
                return;
            }

            for (var i = this.data.Count - 1; i >= 0; --i)
            {
                var bucket = this.data.Values[i];
                if (bucket.EndTime > maxScanTime)
                {
                    break;
                }

                if (bucket.EndTime <= maxBucketAge)
                {
                    this.data.Remove(bucket.StartTime);
                    bucket.PermanentDelete();
                    bucket.Dispose();
                    continue;
                }

                if (bucket.EndTime <= maxUnsealedAge && !bucket.Sealed)
                {
                    bucket.Seal();
                    bucket.Persist(); // Write out sealed data immediately as well. TODO: Remove this pending serialization speed improvements.
                    if (this.OnDataSealed != null)
                    {
                        this.OnDataSealed(bucket.StartTime, bucket.EndTime);
                    }
                }
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability",
            "CA2000:Dispose objects before losing scope")]
        private DataBucket<TInternal> GetOrCreateDataBucket(DateTime timestamp, bool addSourceData)
        {
            if (timestamp < earliestUnsealedBucketTime)
            {
                return null;
            }

            var existingBucket = this.GetDataBucket(timestamp);
            if (existingBucket != null)
            {
                return existingBucket;
            }

            var newBucket = new DataBucket<TInternal>(this.CreateOptimizedDimensionSet(), timestamp,
                                                                     this.properties.CompactionConfiguration
                                                                         .DefaultBucketTicks,
                                                                     this.storagePath,
                                                                     this.properties.MemoryStreamManager);
            Events.Write.CreatingDataBucket(this.Name, newBucket.StartTime, newBucket.EndTime);
            this.AddBucket(newBucket);

            if (addSourceData)
            {
                newBucket.SetSourceAvailable(this.properties.LocalSourceName);
            }

            return newBucket;
        }

        private bool HaveDimension(string dimension)
        {
            return this.DimensionSet.dimensions.Any(d => d.Name.Equals(dimension, StringComparison.OrdinalIgnoreCase));
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability",
            "CA2000:Dispose objects before losing scope")]
        private void CompactBuckets(IList<DataBucket<TInternal>> buckets, DateTime newBucketTimeStamp,
                                    long rolledUpTimeSpanInTicks)
        {
            bool shouldRelease = true;

            var rolledUpBucket = new DataBucket<TInternal>(buckets, this.CreateOptimizedDimensionSet(),
                                                                          newBucketTimeStamp.ToLocalTime(),
                                                                          rolledUpTimeSpanInTicks,
                                                                          this.storagePath,
                                                                          this.properties.MemoryStreamManager);
            rolledUpBucket.Seal();

            using (SharedLock.OpenExclusive(this.dataLock))
            {
                foreach (var dataBucket in buckets)
                {
                    shouldRelease &= !dataBucket.Loaded;
                    if (!this.data.Remove(dataBucket.StartTime))
                    {
                        throw new InvalidOperationException("Double compaction attempted on same bucket: " + this.Name + " " + dataBucket.StartTime.ToString());
                    }
                }

                this.data.Add(rolledUpBucket.StartTime, rolledUpBucket);
            }

            foreach (var dataBucket in buckets)
            {
                dataBucket.PermanentDelete();
                dataBucket.Dispose();
            }

            if (shouldRelease)
            {
                rolledUpBucket.ReleaseData();
            }
        }

        private IEnumerable<DataSample> PopulateBucketedDataSamples(DimensionSpecification filterDims,
                                                                    Action<DataSample, TInternal, QuerySpecification>
                                                                        sampleAction, QuerySpecification querySpec)
        {
            using (SharedLock.OpenShared(this.dataLock))
            {
                var bucketQuery = new BucketQuery(this, filterDims);
                foreach (var bucket in bucketQuery)
                {
                    foreach (var match in (querySpec.IsCrossQuery
                                               ? bucket.GetMatchesSplitByDimension(filterDims,
                                                                                   querySpec.CrossQueryDimension)
                                               : bucket.GetMatches(filterDims)))
                    {
                        if (match.DataCount == 0)
                        {
                            continue;
                        }

                        var sample = new DataSample
                                     {
                                         Name = this.Name,
                                         Dimensions = match.DimensionValues.Data,
                                         StartTime = bucket.StartTime.ToMillisecondTimestamp(),
                                         EndTime = bucket.EndTime.ToMillisecondTimestamp(),
                                     };

                        sampleAction(sample, match.Data, querySpec);
                        yield return sample;
                    }
                }
            }

            Events.Write.EndQueryData(this);
        }

        private struct CombinedSample
        {
            public TInternal Data;
            public DimensionSpecification Dimensions;
        }

        private IEnumerable<DataSample> PopulateCombinedDataSamples(DimensionSpecification filterDims,
            Action<DataSample, TInternal, QuerySpecification> sampleAction, QuerySpecification querySpec)
        {
            var combinedData = new Dictionary<string, CombinedSample>();

            long start = long.MaxValue;
            long end = long.MinValue;

            using (SharedLock.OpenShared(this.dataLock))
            {
                var bucketQuery = new BucketQuery(this, filterDims);
                foreach (var bucket in bucketQuery)
                {
                    foreach (var match in (querySpec.IsCrossQuery
                                               ? bucket.GetMatchesSplitByDimension(filterDims,
                                                                                   querySpec.CrossQueryDimension)
                                               : bucket.GetMatches(filterDims)))
                    {
                        if (match.DataCount == 0)
                        {
                            continue;
                        }

                        CombinedSample value;
                        if (!combinedData.TryGetValue(match.SplitDimensionValue, out value))
                        {
                            value = new CombinedSample
                                    {
                                        Data = match.Data,
                                        Dimensions = match.DimensionValues,
                                    };
                            combinedData[match.SplitDimensionValue] = value;
                        }
                        else
                        {
                            value.Data.MergeFrom(match.Data);
                        }

                        if (bucket.StartTicks < start)
                        {
                            start = bucket.StartTicks;
                        }
                        if (bucket.EndTicks > end)
                        {
                            end = bucket.EndTicks;
                        }
                    }
                }
            }

            foreach (var value in combinedData.Values)
            {
                var sample =
                    new DataSample
                    {
                        Name = this.Name,
                        StartTime =
                            new DateTime(start, DateTimeKind.Utc).ToMillisecondTimestamp(),
                        EndTime =
                            new DateTime(end, DateTimeKind.Utc).ToMillisecondTimestamp(),
                        Dimensions = value.Dimensions.Data,
                    };

                sampleAction(sample, value.Data, querySpec);

                yield return sample;
            }

            Events.Write.EndQueryData(this);
        }

        private sealed class BucketQuery : IEnumerable<DataBucket<TInternal>>
        {
            private struct BucketPair
            {
                public DataBucket<TInternal> Bucket;
                public bool WasLoaded;
            }

            private readonly List<BucketPair> buckets = new List<BucketPair>();
            private readonly DataSet<TInternal> dataSet;

            public BucketQuery(DataSet<TInternal> dataSet, DimensionSpecification filterDimensions)
            {
                this.dataSet = dataSet;

                DateTime start, end;
                dataSet.GetTimesFromDimensions(filterDimensions, out start, out end);

                foreach (var bucket in dataSet.data.Values)
                {
                    if (bucket.StartTicks < start.Ticks || bucket.EndTicks > end.Ticks)
                    {
                        continue;
                    }

                    this.buckets.Add(new BucketPair {Bucket = bucket, WasLoaded = bucket.Loaded});
                }
            }

            public IEnumerator<DataBucket<TInternal>> GetEnumerator()
            {
                foreach (var pair in this.buckets)
                {
                    yield return pair.Bucket;
                    if (!pair.WasLoaded)
                    {
                        pair.Bucket.ReleaseData();
                    }
                }
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }
        }

        private static void CheckPercentileValue(double percentile)
        {
            if (double.IsNaN(percentile) || percentile < 0 || percentile > 100)
            {
                throw new ArgumentOutOfRangeException("percentile");
            }
        }

        private void GetTimesFromDimensions(DimensionSpecification dims, out DateTime start, out DateTime end)
        {
            start = DateTime.MinValue;
            end = DateTime.MaxValue;

            var userSpecifiedStartTime = false;
            var userSpecifiedEndTime = false;

            string time;
            if (dims.TryGetValue(ReservedDimensions.StartTimeDimension, out time))
            {
                start = DateTime.Parse(time).ToUniversalTime();
                userSpecifiedStartTime = true;
            }
            else if (this.data.Count > 0)
            {
                // Find the oldest data we have loaded.
                DateTime earliest = this.data.Values[0].StartTime;
                for (var i = 0; i < this.data.Count; ++i)
                {
                    var bucket = this.data.Values[i];
                    if (!bucket.Loaded)
                    {
                        break;
                    }
                    earliest = bucket.StartTime;
                }
                start = earliest;
            }

            if (dims.TryGetValue(ReservedDimensions.EndTimeDimension, out time))
            {
                end = DateTime.Parse(time).ToUniversalTime();
                userSpecifiedEndTime = true;
            }
            else if (this.data.Count > 0)
            {
                end = this.data.Values[0].EndTime;
            }

            if (start >= end)
            {
                // if the user asked for one half of the time range [start -> inf] or [inf -> end] we will not 
                // penalize them with an argument exception if we don't have that time bucket
                if (!userSpecifiedStartTime || !userSpecifiedEndTime)
                {
                    // guarantee there will be no results
                    start = DateTime.MaxValue;
                    end = DateTime.MaxValue;
                    return;
                }

                throw new ArgumentOutOfRangeException("start", "Start time is greater than or equal to end time.");
            }
        }

        private DimensionSet CreateOptimizedDimensionSet()
        {
            // Create a shallow-copy DimensionSet. If we've currently got data we use the latest data as a hint to
            // order the dimensions in the new set. This can greatly improve the performance of the KeyTree since the
            // most populous dimensions are floated closer to the top of the tree.
            return this.data.Count > 0
                       ? new DimensionSet(this.data.Values[0].DimensionSet)
                       : this.DimensionSet;
        }

        public void Dispose()
        {
            if (this.dataLock != null)
            {
                using (SharedLock.OpenExclusive(this.dataLock))
                {
                    foreach (var bucket in this.data.Values)
                    {
                        bucket.Dispose();
                    }

                    this.data.Clear();
                }

                this.dataLock.Dispose();
                this.dataLock = null;
            }
        }

        #region IDataSet
        public IEnumerable<DataSample> QueryData(DimensionSpecification filterDims, QuerySpecification querySpec)
        {
            Events.Write.BeginQueryData(this, filterDims, querySpec);

            if (filterDims == null)
            {
                throw new ArgumentNullException("filterDims");
            }

            // EndQueryData has to be called in the data enumerators because the enumeration does not occur
            // until iteration begins on the IEnumerable we return.

            if (querySpec.QueryType != QueryType.Normal && typeof(TInternal) != typeof(InternalHistogram))
            {
                throw new NotSupportedException("Cannot get percentiles for non-histograms.");
            }

            switch (querySpec.QueryType)
            {
            case QueryType.Normal:
            {
                return (querySpec.Combine
                            ? this.PopulateCombinedDataSamples(filterDims, PopulateNormalSample, querySpec)
                            : this.PopulateBucketedDataSamples(filterDims, PopulateNormalSample, querySpec));
            }
            case QueryType.Percentile:
            {
                CheckPercentileValue(querySpec.Percentile);

                return (querySpec.Combine
                            ? this.PopulateCombinedDataSamples(filterDims, PopulatePercentileSample, querySpec)
                            : this.PopulateBucketedDataSamples(filterDims, PopulatePercentileSample, querySpec));
            }
            case QueryType.Average:
            {
                return (querySpec.Combine
                            ? this.PopulateCombinedDataSamples(filterDims, PopulateAverageSample, querySpec)
                            : this.PopulateBucketedDataSamples(filterDims, PopulateAverageSample, querySpec));
            }
            case QueryType.Maximum:
            {
                return (querySpec.Combine
                            ? this.PopulateCombinedDataSamples(filterDims, PopulateMaximumSample, querySpec)
                            : this.PopulateBucketedDataSamples(filterDims, PopulateMaximumSample, querySpec));
            }
            case QueryType.Minimum:
            {
                return (querySpec.Combine
                            ? this.PopulateCombinedDataSamples(filterDims, PopulateMinimumSample, querySpec)
                            : this.PopulateBucketedDataSamples(filterDims, PopulateMinimumSample, querySpec));
            }
            default:
                throw new NotSupportedException("Unknown query type " + querySpec.QueryType);
            }
        }

        private static void PopulateNormalSample(DataSample sample, TInternal data, QuerySpecification querySpec)
        {
            data.UpdateDataSample(sample);
        }

        private static void PopulateAverageSample(DataSample sample, TInternal data,
                                                  QuerySpecification querySpec)
        {
            var histogram = data as InternalHistogram;
            sample.SampleCount = histogram.SampleCount;
            sample.SampleType = DataSampleType.Average;
            sample.Average = histogram.Data.GetAverageValue(histogram.SampleCount);
        }

        private static void PopulatePercentileSample(DataSample sample, TInternal data,
                                                     QuerySpecification querySpec)
        {
            var histogram = data as InternalHistogram;
            sample.SampleCount = histogram.SampleCount;
            sample.SampleType = DataSampleType.Percentile;
            sample.Percentile = querySpec.Percentile;
            sample.PercentileValue = histogram.Data.GetValueAtPercentile(querySpec.Percentile, sample.SampleCount);
        }

        private static void PopulateMaximumSample(DataSample sample, TInternal data,
                                                  QuerySpecification querySpec)
        {
            var histogram = data as InternalHistogram;
            sample.SampleType = DataSampleType.Maximum;
            sample.SampleCount = histogram.SampleCount;
            sample.MaxValue = histogram.Data.GetMaximumValue();
        }

        private static void PopulateMinimumSample(DataSample sample, TInternal data,
                                                  QuerySpecification querySpec)
        {
            var histogram = data as InternalHistogram;
            sample.SampleType = DataSampleType.Minimum;
            sample.SampleCount = histogram.SampleCount;
            sample.MinValue = histogram.Data.GetMinimumValue();
        }

        public IEnumerable<string> GetDimensions()
        {
            return this.DimensionSet.Dimensions.Select(d => d.Name);
        }

        private delegate DateTime GetTimestamp(DataBucket<TInternal> bucket);

        private IEnumerable<string> GetTimestampValues(GetTimestamp getter)
        {
            using (SharedLock.OpenShared(this.dataLock))
            {
                foreach (var bucket in this.data.Values)
                {
                    yield return getter(bucket).ToString(Protocol.TimestampStringFormat);
                }
            }
        }

        public IEnumerable<string> GetDimensionValues(string dimensionName, DimensionSpecification filterDims)
        {
            if (ReservedDimensions.StartTimeDimension.Equals(dimensionName, StringComparison.OrdinalIgnoreCase))
            {
                return this.GetTimestampValues(b => b.StartTime);
            }
            if (ReservedDimensions.EndTimeDimension.Equals(dimensionName, StringComparison.OrdinalIgnoreCase))
            {
                return this.GetTimestampValues(b => b.EndTime);
            }

            var values = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            if (!this.HaveDimension(dimensionName))
            {
                throw new KeyNotFoundException(dimensionName);
            }

            // If all dimensions are provided we'll end up just adding the single value back for the given dimension
            // and pushing that out -- is this a neat hack to test if a dimension value exists, or is it ridiculous?
            // Going with ridiculous today.
            int matched = (from dim in filterDims.Keys where this.HaveDimension(dim) select dim).Count();
            if (matched == this.DimensionSet.dimensions.Length)
            {
                throw new ArgumentException("All known dimensions were supplied in filter.", "filterDims");
            }

            using (SharedLock.OpenShared(this.dataLock))
            {
                var bucketQuery = new BucketQuery(this, filterDims);
                foreach (var bucket in bucketQuery)
                {
                    foreach (var value in bucket.GetDimensionValues(dimensionName, filterDims))
                    {
                        values.Add(value);
                    }
                }
            }

            return values;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability",
            "CA2000:Dispose objects before losing scope")]
        public void LoadStoredData()
        {
            if (this.storagePath == null)
            {
                return;
            }

            DateTime start, end;
            var files = (from filename in Directory.EnumerateFiles(this.storagePath)
                where DataBucket<TInternal>.GetTimestampsFromFullFilename(filename, out start, out end)
                orderby filename.ToLowerInvariant() ascending
                select filename).ToList();

            foreach (var file in files)
            {
                var bucket = new DataBucket<TInternal>(new DimensionSet(this.DimensionSet), file,
                                                                      this.properties.MemoryStreamManager, null);
                // If we crash or are killed during compaction it is possible that both the uncompacted and
                // compacted data files are still on the disk. In this case we prefer sticking with the compacted
                // files.
                if (this.GetDataBucket(bucket.StartTime) != null)
                {
                    bucket.PermanentDelete();
                    bucket.Dispose();
                    continue;
                }
                this.AddBucket(bucket);
            }

            // Load the last bucket into memory (as it is most likely to be next used, and can help inform us of how
            // to optimize future additions of data).
            // For all unsealed buckets load their sources into memory as well.
            if (this.data.Count > 0)
            {
                this.data.Values[0].LoadData();
                foreach (var b in from bucket in this.data.Values where !bucket.Sealed select bucket)
                {
                    b.LoadSources();
                }
            }
        }

        public bool ReleaseOldestData(bool releaseLatest)
        {
            DataBucket<TInternal> releaseBucket = null;

            // Under heavy stress we want to unblock even if we can't find data to release.
            using (SharedLock.OpenShared(this.dataLock))
            {
                for (var i = this.data.Count - 1; i >= (releaseLatest ? 0 : 1); --i)
                {
                    var bucket = this.data.Values[i];
                    if (bucket.Loaded)
                    {
                        releaseBucket = bucket;
                        break;
                    }
                }
            }

            if (releaseBucket != null)
            {
                releaseBucket.ReleaseData();
                return true;
            }

            return false;
        }

        public bool SupportsAverageQuery
        {
            get
            {
                return this.SupportsPercentileQuery; // same thing for now. Could change in future.
            }
        }

        public bool SupportsPercentileQuery
        {
            get { return (typeof(TInternal) == typeof(InternalHistogram)); }
        }

        public PersistedDataType PersistedDataType
        {
            get
            {
                return PersistedDataProtocol.GetPersistedTypeCodeFromType(typeof(TInternal));
            }
        }

        public bool Serialize(DateTime startTime, DateTime endTime, Stream destination)
        {
            // When serializing, if asked to send data for multiple buckets, we simply have them all serialize one-by-
            // one into the stream. The reader fully supports this behavior. We could potentially save wire size by
            // condensing all buckets together, but the caller may not want that and the CPU cost would be quite
            // substantial to do so.

            startTime = startTime.ToUniversalTime();
            endTime = endTime.ToUniversalTime();

            var serializeBuckets = new List<DataBucket<TInternal>>();
            using (SharedLock.OpenShared(this.dataLock))
            {
                foreach (var bucketPair in this.data)
                {
                    var bucketTimestamp = bucketPair.Key;
                    if (bucketTimestamp < startTime || bucketTimestamp >= endTime)
                    {
                        continue;
                    }

                    serializeBuckets.Add(bucketPair.Value);
                }
            }

            foreach (var bucket in serializeBuckets)
            {
                bucket.Serialize(this.Name, destination);
            }

            return serializeBuckets.Count > 0;
        }

        public void Compact()
        {
            // Ensure only one call to Compact is happening at a time. 
            // If a second one comes in and loses the race that's fine. 
            if (Interlocked.CompareExchange(ref this.compactionInProgress, 1, 0) == 0)
            {
                this.DoCompaction();
                Interlocked.Exchange(ref this.compactionInProgress, 0);
            }
        }

        private void DoCompaction()
        {
            var compactionQueue = new Dictionary<DateTime, CompactionSet<TInternal>>();
            using (SharedLock.OpenShared(this.dataLock))
            {
                if (!this.properties.CompactionConfiguration.IsCompactionEnabled || this.data.Count == 0)
                {
                    return;
                }

                var bucketGroups =
                    this.properties.CompactionConfiguration.GetEarliestTimestampsPerBucket(this.latestStartTime);
                if (bucketGroups.Count < 2)
                {
                    return; // no possible work.
                }

                for (var i = this.data.Count - 1; i >= 0; --i)
                {
                    var bucket = this.data.Values[i];

                    if (!bucket.Sealed)
                    {
                        break;
                    }

                    DataIntervalConfiguration correctBucketConfig = null;
                    foreach (var kvp in bucketGroups)
                    {
                        var minTimestamp = kvp.Key;
                        var bucketConfig = kvp.Value;

                        correctBucketConfig = bucketConfig;
                        if (minTimestamp <= bucket.StartTime)
                        {
                            break; // previously set configuration is correct.
                        }
                    }

                    if (bucket.TimeSpan < correctBucketConfig.Interval)
                    {
                        CompactionSet<TInternal> compactionSet;

                        var targetTimestamp =
                            correctBucketConfig.ConvertToAlternateBucketDuration(bucket.StartTime);
                        if (!compactionQueue.TryGetValue(targetTimestamp, out compactionSet))
                        {
                            compactionSet = new CompactionSet<TInternal>
                                            {
                                                SourceBucketDuration = bucket.TimeSpan,
                                                TargetTimestamp = targetTimestamp,
                                                TargetIntervalConfiguration = correctBucketConfig,
                                            };
                            compactionQueue.Add(targetTimestamp, compactionSet);
                        }
                        compactionSet.BucketSet.Add(bucket);
                    }
                }
            }

            foreach (var compactionSet in compactionQueue.Values)
            {
                if (this.properties.ShuttingDown)
                {
                    break;
                }

                Events.Write.BeginCompactingData(this, compactionSet.TargetIntervalConfiguration.Interval,
                                                 compactionSet.TargetTimestamp.Ticks);
                this.CompactBuckets(compactionSet.BucketSet, compactionSet.TargetTimestamp,
                                    compactionSet.TargetIntervalConfiguration.Interval.Ticks);
                Events.Write.EndCompactingData(this);
            }
        }


        public void SetLatestTimeForDataSources(SortedList<DateTimeOffset, List<string>> sourcesByTime)
        {
            Events.Write.BeginSetDataSourceTimes(this.Name);
            using (SharedLock.OpenExclusive(this.dataLock))
            {
                foreach (var kvp in sourcesByTime)
                {
                    var latestTime = kvp.Key;
                    var serverList = kvp.Value;

                    if (latestTime < this.earliestUnsealedBucketTime)
                    {
                        continue;
                    }

                    DateTimeOffset bucketTimestamp =
                        this.earliestUnsealedBucketTime == DateTime.MinValue
                            ? latestTime - this.properties.SealTime
                            : this.earliestUnsealedBucketTime;

                    while (bucketTimestamp < latestTime)
                    {
                        // we ensure above that we won't ask for a time that is too early, so bucket is guaranteed to be
                        // non-null.
                        var bucket = this.GetOrCreateDataBucket(bucketTimestamp.UtcDateTime, false);
                        bucketTimestamp += this.properties.CompactionConfiguration.Default.Interval;
                        foreach (var server in serverList)
                        {
                            bucket.AddDataSource(server);
                        }
                    }
                }
            }
            Events.Write.EndSetDataSourceTimes(this.Name);
        }

        public PendingData GetNextPendingData(DateTimeOffset previousStartTime)
        {
            DataBucket<TInternal> pendingBucket = null;
            IList<string> pendingSources = null;

            using (SharedLock.OpenShared(this.dataLock))
            {
                for (var i = this.data.Count - 1; i >= 0; --i)
                {
                    var bucket = this.data.Values[i];
                    if (bucket.Sealed || bucket.StartTime <= previousStartTime)
                    {
                        continue;
                    }

                    var sources = bucket.GetPendingSources();
                    if (sources.Count == 0)
                    {
                        continue;
                    }

                    if (pendingBucket == null)
                    {
                        pendingBucket = bucket;
                        pendingSources = sources;
                    }
                    else if (sources.Count > pendingSources.Count)
                    {
                        pendingBucket = bucket;
                        pendingSources = sources;
                    }
                }
            }

            if (pendingBucket != null)
            {
                return new PendingData
                       {
                           // These are guaranteed UTC.
                           StartTime = new DateTimeOffset(pendingBucket.StartTime, TimeSpan.Zero),
                           EndTime = new DateTimeOffset(pendingBucket.EndTime, TimeSpan.Zero),
                           Sources = pendingSources,
                       };
            }

            // It may be that we have older pending data for them to work on, if we couldn't find anything try again
            // with the minimum start time.
            return previousStartTime != DateTimeOffset.MinValue
                       ? this.GetNextPendingData(DateTimeOffset.MinValue)
                       : null;
        }

        public void UpdateFromAggregator(IPersistedDataAggregator aggregator, DateTimeOffset start, DateTimeOffset end)
        {
            DataBucket<TInternal> updateBucket = null;

            using (SharedLock.OpenShared(this.dataLock))
            {
                foreach (var bucket in this.data.Values)
                {
                    if (bucket.StartTime == start && bucket.EndTime == end)
                    {
                        updateBucket = bucket;
                        break;
                    }
                }
            }

            if (updateBucket == null)
            {
                Events.Write.UnknownBucketCannotBeUpdated(this.Name, start, end);
                return;
            }

            if (updateBucket.Sealed)
            {
                Events.Write.SealedBucketCannotBeUpdated(this.Name, start, end);
                return;
            }

            var agg = aggregator as PersistedDataAggregator<TInternal>;
            var availableSources = new List<string>();

            foreach (var source in agg.Sources)
            {
                switch (source.Status)
                {
                case PersistedDataSourceStatus.Unavailable:
                    updateBucket.SetSourceUnavailable(source.Name);
                    break;
                case PersistedDataSourceStatus.Available:
                    availableSources.Add(source.Name);
                    break;
                case PersistedDataSourceStatus.Unknown:
                    break;
                default:
                    throw new ArgumentException("Unexpected source status " + source.Status, "aggregator");
                }
            }

            if (availableSources.Count > 0)
            {
                var aggregateData = agg.AcquireData();
                updateBucket.UpdateDataFromSources(availableSources, agg.DimensionSet, aggregateData);
            }

            // XXX: Dump data back to disk for now (eases memory pressure)
            updateBucket.ReleaseData();
        }

        public void Flush()
        {
            using (SharedLock.OpenShared(this.dataLock))
            {
                foreach (var b in this.data.Values)
                {
                    b.Flush();
                }
            }
        }
        #endregion
    }
}
