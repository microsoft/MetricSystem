// ---------------------------------------------------------------------
// <copyright file="IDataSet.cs" company="Microsoft">
//     Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//     Information Contained Herein is Proprietary and Confidential.
// </copyright>
// --------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    internal delegate void DataSealedHandler(DateTime startTime, DateTime endTime);

    internal sealed class PendingData
    {
        public DateTimeOffset StartTime;
        public DateTimeOffset EndTime;
        public IList<string> Sources;
    }

    internal interface IDataSet : IDisposable
    {
        IEnumerable<DataSample> QueryData(DimensionSpecification filterDims, QuerySpecification querySpec);
        IEnumerable<string> GetDimensions();
        IEnumerable<string> GetDimensionValues(string dimensionName, DimensionSpecification filterDims);
        void LoadStoredData();
        /// <summary>
        /// Release the oldest loaded data. If there is any available to release.
        /// </summary>
        /// <param name="releaseLatest">True if the latest chunk of data should be released as well.</param>
        /// <returns>True if releasable data was found.</returns>
        bool ReleaseOldestData(bool releaseLatest);
        string Name { get; }
        DimensionSet DimensionSet { get; }
        bool SupportsAverageQuery { get; }
        bool SupportsPercentileQuery { get; }
        bool Serialize(DateTime startTime, DateTime endTime, Stream destination);
        event DataSealedHandler OnDataSealed;
        void Compact();
        void Flush();

        /// <summary>
        /// Earliest known time for queryable counter data.
        /// </summary>
        DateTimeOffset StartTime { get; }

        /// <summary>
        /// Latest known time for queryable counter data.
        /// </summary>
        DateTimeOffset EndTime { get; }

        PersistedDataType PersistedDataType { get; }

        /// <summary>
        /// Set the latest known time that data is available for a set of sources, ordered by time.
        /// Creates new data buckets for all unsealed times starting backwards from the latest time.
        /// </summary>
        /// <param name="sourcesByTime">Lists of servers paired with their latest timestamp.</param>
        void SetLatestTimeForDataSources(SortedList<DateTimeOffset, List<string>> sourcesByTime);

        /// <summary>
        /// Get information on the oldest pending data with the most unfulfilled sources.
        /// </summary>
        /// <param name="previousStartTime">Previous start time to advance ahead from.</param>
        /// <returns>The oldest pending data, or null if there is no data waiting to be fulfilled.</returns>
        PendingData GetNextPendingData(DateTimeOffset previousStartTime);

        /// <summary>
        /// Ingest data from aggregator.
        /// </summary>
        /// <param name="aggregator">Aggregator with source data.</param>
        /// <param name="start">Start time of aggregated data window.</param>
        /// <param name="end">End time of aggregated data window.</param>
        void UpdateFromAggregator(IPersistedDataAggregator aggregator, DateTimeOffset start, DateTimeOffset end);
    }
}
