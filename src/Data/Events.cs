// ---------------------------------------------------------------------
// <copyright file="Events.cs" company="Microsoft">
//       Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Diagnostics.Tracing;
    using System.Linq;

    [EventSource(Name = "MetricSystem-Data", Guid = "{40dda421-969e-493c-b33f-64c54157865c}")]
    public sealed class Events : EventSource
    {
        public static readonly Events Write = new Events();

        [NonEvent]
        public void SealTimeSet(TimeSpan timeSpan)
        {
            if (this.IsEnabled(EventLevel.Informational, EventKeywords.None))
            {
                this.SealTimeSet(timeSpan.ToString());
            }
        }
        [Event(1, Level = EventLevel.Informational)]
        private void SealTimeSet(string timeSpan)
        {
            this.WriteEvent(1, timeSpan);
        }

        [NonEvent]
        public void MaximumAgeSet(TimeSpan timeSpan)
        {
            if (this.IsEnabled(EventLevel.Informational, EventKeywords.None))
            {
                this.MaximumAgeSet(timeSpan.ToString());
            }
        }
        [Event(2, Level = EventLevel.Informational)]
        private void MaximumAgeSet(string timeSpan)
        {
            this.WriteEvent(2, timeSpan);
        }

        [Event(100, Level = EventLevel.Warning)]
        internal void UnknownDataRequested(string dataName)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(100, dataName);
            }
        }

        [Event(101, Level = EventLevel.Verbose)]
        internal void WrongTypeForCounterRequested(string counterName, string expectedType, string actualType)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(101, counterName, expectedType, actualType);
            }
        }

        [Event(102, Level = EventLevel.Informational)]
        internal void BeginHeapCleanup(ulong currentPrivateMemorySizeMB, ulong preferredPrivateMemoryMaxMB,
                                       ulong maxPrivateMemorySizeMB)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(102, currentPrivateMemorySizeMB, preferredPrivateMemoryMaxMB, maxPrivateMemorySizeMB);
            }
        }

        [Event(103, Level = EventLevel.Informational)]
        internal void EndHeapCleanup(ulong currentPrivateMemorySizeMB)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(103, currentPrivateMemorySizeMB);
            }
        }

        [Event(104, Level = EventLevel.Informational)]
        internal void ShuttingDownDueToHighMemoryUsage(ulong currentPrivateMemorySizeMB, ulong maxPrivateMemorySizeMB)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(104, currentPrivateMemorySizeMB, maxPrivateMemorySizeMB);
            }
        }

        [Event(105, Level = EventLevel.Informational)]
        internal void BeginPerformGarbageCollection()
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(105);
            }
        }

        [Event(106, Level = EventLevel.Informational)]
        internal void EndPerformGarbageCollection()
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(106);
            }
        }

        [Event(107, Level = EventLevel.Verbose)]
        internal void BeginPeriodicMaintenance()
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(107);
            }
        }

        [Event(108, Level = EventLevel.Verbose)]
        internal void EndPeriodicMaintenance()
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(108);
            }
        }

        [Event(110, Level = EventLevel.Verbose)]
        internal void BeginLoadingData(string filename)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(110, filename);
            }
        }

        [Event(111, Level = EventLevel.Verbose)]
        internal void EndLoadingData(string filename)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(111, filename);
            }
        }

        [Event(112, Level = EventLevel.Verbose)]
        internal void BeginWritingData(string filename)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(112, filename);
            }
        }

        [Event(113, Level = EventLevel.Verbose)]
        internal void EndWritingData(string filename)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(113, filename);
            }
        }

        [NonEvent]
        internal void BeginCompactingData(IDataSet dataSet, TimeSpan targetTimespan, long timestampTicksUtc)
        {
            if (this.IsEnabled(EventLevel.Informational, EventKeywords.None))
            {
                this.BeginCompactingData(dataSet.Name, targetTimespan.ToString(),
                                         new DateTime(timestampTicksUtc).ToString("o"));
            }
        }

        [Event(120, Level = EventLevel.Informational)]
        private void BeginCompactingData(string dataSet, string targetTimespan, string timestamp)
        {
            this.WriteEvent(120, dataSet, targetTimespan, timestamp);
        }

        [NonEvent]
        internal void EndCompactingData(IDataSet dataSet)
        {
            if (this.IsEnabled(EventLevel.Informational, EventKeywords.None))
            {
                this.EndCompactingData(dataSet.Name);
            }
        }

        [Event(121, Level = EventLevel.Informational)]
        private void EndCompactingData(string dataSet)
        {
            this.WriteEvent(121, dataSet);
        }

        [NonEvent]
        internal void BeginQueryData(IDataSet dataSet, DimensionSpecification filterDimensions,
                                     QuerySpecification querySpec)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.BeginQueryData(dataSet.Name,
                                    string.Join("; ", (from pair in filterDimensions select pair.Key + '=' + pair.Value)),
                                    querySpec.ToString());
            }
        }

        [Event(130, Level = EventLevel.Verbose)]
        private void BeginQueryData(string dataSet, string filterDimensions, string querySpec)
        {
            this.WriteEvent(130, dataSet, filterDimensions, querySpec);
        }

        [NonEvent]
        internal void EndQueryData(IDataSet dataSet)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.EndQueryData(dataSet.Name);
            }
        }

        [Event(131, Level = EventLevel.Verbose)]
        private void EndQueryData(string dataSet)
        {
            this.WriteEvent(131, dataSet);
        }

        [Event(140, Level = EventLevel.Verbose)]
        public void BeginSetDataSourceTimes(string name)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(140, name);
            }
        }

        [Event(141, Level = EventLevel.Verbose)]
        public void EndSetDataSourceTimes(string name)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(141, name);
            }
        }

        [NonEvent]
        public void UnknownBucketCannotBeUpdated(string dataSetName, DateTimeOffset startTime, DateTimeOffset endTime)
        {
            if (this.IsEnabled(EventLevel.Warning, EventKeywords.None))
            {
                this.UnknownBucketCannotBeUpdated(dataSetName, startTime.ToString("o"), endTime.ToString("o"));
            }
        }
        [Event(142, Level = EventLevel.Warning)]
        private void UnknownBucketCannotBeUpdated(string dataSetName, string startTime, string endTime)
        {
            this.WriteEvent(142, dataSetName, startTime, endTime);
        }

        [NonEvent]
        public void SealedBucketCannotBeUpdated(string dataSetName, DateTimeOffset startTime, DateTimeOffset endTime)
        {
            if (this.IsEnabled(EventLevel.Warning, EventKeywords.None))
            {
                this.SealedBucketCannotBeUpdated(dataSetName, startTime.ToString("o"), endTime.ToString("o"));
            }
        }
        [Event(143, Level = EventLevel.Warning)]
        private void SealedBucketCannotBeUpdated(string dataSetName, string startTime, string endTime)
        {
            this.WriteEvent(143, dataSetName, startTime, endTime);
        }

        [NonEvent]
        internal void RejectedAttemptToWriteAncientData(string dataSetName, DateTime minimumTime, DateTime attemptedTime)
        {
            this.RejectedAttemptToWriteAncientData(dataSetName, minimumTime.Ticks, attemptedTime.Ticks);
        }

        [Event(200, Level = EventLevel.Warning)]
        private void RejectedAttemptToWriteAncientData(string dataSetName, long minUTCTicks, long attemptedUTCTicks)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(200, dataSetName, minUTCTicks, attemptedUTCTicks);
            }
        }

        [Event(201, Level = EventLevel.Warning,
            Message = "File {0} is missing. It was likely removed by the disk size quota manager.")]
        internal void SealedDataFileMissing(string filename)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(201, filename);
            }
        }

        [Event(202, Level = EventLevel.Verbose)]
        internal void DeletingDataBucketFromLocalStorage(string filename)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(202, filename);
            }
        }

        [Event(203, Level = EventLevel.Warning)]
        internal void FailedToDeleteDataBucketFromLocalStorage(string filename)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(203, filename);
            }
        }

        [Event(204, Level = EventLevel.Warning,
            Message = "File {0} is incomplete. It's contents are being discarded")]
        internal void DiscardingIncompleteData(string filename)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(204, filename);
            }
        }

        [NonEvent]
        internal void PersistedDataException(Exception ex)
        {
            if (ex == null)
            {
                throw new ArgumentNullException("ex");
            }

            if (this.IsEnabled(EventLevel.Warning, EventKeywords.None))
            {
                while (ex != null)
                {
                    this.PersistedDataException(ex.GetType().ToString(), ex.Message, ex.StackTrace);
                    ex = ex.InnerException;
                }
            }
        }

        [Event(205, Level = EventLevel.Warning)]
        private void PersistedDataException(string type, string message, string stackTrace)
        {
            this.WriteEvent(205, type, message, stackTrace);
        }

        [Event(206, Level = EventLevel.Verbose)]
        internal void BeginReadPersistedDataHeader()
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(206);
            }
        }

        [Event(207, Level = EventLevel.Verbose)]
        internal void EndReadPersistedDataHeader()
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(207);
            }
        }

        [Event(208, Level = EventLevel.Verbose)]
        internal void BeginReadPersistedData()
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(208);
            }
        }

        [Event(209, Level = EventLevel.Verbose)]
        internal void EndReadPersistedData()
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(209);
            }
        }

        [NonEvent]
        public void CreatingDataBucket(string counterName, DateTime startTime, DateTime endTime)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.CreatingDataBucket(counterName, startTime.ToString("o"), endTime.ToString("o"));
            }
        }
        [Event(210, Level = EventLevel.Verbose)]
        private void CreatingDataBucket(string counterName, string startTime, string endTime)
        {
            this.WriteEvent(210, counterName, startTime, endTime);
        }

        [Event(211, Level = EventLevel.Critical)]
        public void ObjectFinalized(string type, string allocationStack)
        {
            this.WriteEvent(211, type, allocationStack);
        }

        [Event(300, Level = EventLevel.Error,
            Message =
                "A compaction operation was performed to build new bucket of quantum {1} ticks with source bucket of quantum {0} ticks."
            )]
        public void AttemptToCompactIncompatibleQuanta(long sourceQuantumTicks, long destQuantumTicks)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(300, sourceQuantumTicks, destQuantumTicks);
            }
        }

        [NonEvent]
        internal void BeginPersistedDataAggregation(string counterName, DateTime startTime, DateTime endTime,
                                                    IEnumerable<PersistedDataSource> sources)
        {
            if (this.IsEnabled(EventLevel.Verbose, Keywords.PersistedDataAggregator))
            {
                this.BeginPersistedDataAggregation(counterName, startTime.ToString("o"), endTime.ToString("o"),
                                                   string.Join(", ", (from s in sources select s.Name)));
            }
        }

        [Event(400, Level = EventLevel.Verbose, Keywords = Keywords.PersistedDataAggregator)]
        private void BeginPersistedDataAggregation(string counterName, string startTime, string endTime, string sources)
        {
            this.WriteEvent(400, counterName, startTime, endTime, sources);
        }

        [Event(401, Level = EventLevel.Verbose, Keywords = Keywords.PersistedDataAggregator)]
        internal void EndPersistedDataAggregation(bool success)
        {
            if (this.IsEnabled(EventLevel.Verbose, Keywords.PersistedDataAggregator))
            {
                this.WriteEvent(401, success);
            }
        }

        [NonEvent]
        internal void BeginQuerySources(IEnumerable<PersistedDataSource> sources)
        {
            if (this.IsEnabled(EventLevel.Verbose, Keywords.PersistedDataAggregator))
            {
                this.BeginQuerySources(string.Join(", ", (from s in sources select s.Name)));
            }
        }

        [Event(402, Level = EventLevel.Verbose, Keywords = Keywords.PersistedDataAggregator)]
        private void BeginQuerySources(string sources)
        {
            this.WriteEvent(402, sources);
        }

        [NonEvent]
        internal void EndQuerySources(bool success, IEnumerable<PersistedDataSource> sources)
        {
            if (this.IsEnabled(EventLevel.Verbose, Keywords.PersistedDataAggregator))
            {
                this.EndQuerySources(success, string.Join(", ", (from s in sources select s.Name + ' ' + s.Status)));
            }
        }

        [Event(403, Level = EventLevel.Verbose, Keywords = Keywords.PersistedDataAggregator)]
        private void EndQuerySources(bool success, string sources)
        {
            this.WriteEvent(403, success, sources);
        }

        [NonEvent]
        internal void BeginSendSourceQuery(PersistedDataSource source, Uri uri)
        {
            if (this.IsEnabled(EventLevel.Verbose, Keywords.PersistedDataAggregator))
            {
                this.BeginSendSourceQuery(source.Name, uri.ToString());
            }
        }

        [Event(404, Level = EventLevel.Verbose, Keywords = Keywords.PersistedDataAggregator)]
        private void BeginSendSourceQuery(string source, string uri)
        {
            this.WriteEvent(404, source, uri);
        }

        [NonEvent]
        internal void EndSendSourceQuery(PersistedDataSource source, int status, string message)
        {
            if (this.IsEnabled(EventLevel.Verbose, Keywords.PersistedDataAggregator))
            {
                this.EndSendSourceQuery(source.Name, status, message);
            }
        }

        [Event(405, Level = EventLevel.Verbose, Keywords = Keywords.PersistedDataAggregator)]
        private void EndSendSourceQuery(string source, int status, string message)
        {
            this.WriteEvent(405, source, status, message);
        }

        [NonEvent]
        internal void BeginReceiveSourceResponseBody(PersistedDataSource source)
        {
            if (this.IsEnabled(EventLevel.Verbose, Keywords.PersistedDataAggregator))
            {
                this.BeginReceiveSourceResponseBody(source.Name);
            }
        }

        [Event(406, Level = EventLevel.Verbose, Keywords = Keywords.PersistedDataAggregator)]
        internal void BeginReceiveSourceResponseBody(string source)
        {
            this.WriteEvent(406, source);
        }

        [NonEvent]
        internal void EndReceiveSourceResponseBody(PersistedDataSource source, int length)
        {
            if (this.IsEnabled(EventLevel.Verbose, Keywords.PersistedDataAggregator))
            {
                this.EndReceiveSourceResponseBody(source.Name, length);
            }
        }

        [Event(407, Level = EventLevel.Verbose, Keywords = Keywords.PersistedDataAggregator)]
        private void EndReceiveSourceResponseBody(string source, int length)
        {
            this.WriteEvent(407, source, length);
        }

        [NonEvent]
        internal void PersistedDataExceptionFromSource(PersistedDataSource source, Exception ex)
        {
            if (this.IsEnabled(EventLevel.Verbose, Keywords.PersistedDataAggregator))
            {
                if (ex.InnerException != null)
                {
                    this.PersistedDataExceptionFromSource(source, ex.InnerException);
                }

                this.PersistedDataExceptionFromSource(source.Name, ex.GetType().ToString(), ex.Message, ex.StackTrace);
            }
        }

        [Event(408, Level = EventLevel.Verbose, Keywords = Keywords.PersistedDataAggregator)]
        private void PersistedDataExceptionFromSource(string source, string type, string message, string stackTrace)
        {
            this.WriteEvent(408, source, type, message, stackTrace);
        }

        [NonEvent]
        internal void HttpExceptionFromSource(PersistedDataSource source, Exception ex)
        {
            if (this.IsEnabled(EventLevel.Verbose, Keywords.PersistedDataAggregator))
            {
                this.HttpExceptionFromSource(source.Name, ex.Message);
            }
        }

        [Event(409, Level = EventLevel.Verbose, Keywords = Keywords.PersistedDataAggregator)]
        private void HttpExceptionFromSource(string source, string message)
        {
            this.WriteEvent(409, source, message);
        }

        [NonEvent]
        internal void EmptyResponseReceivedFromSource(PersistedDataSource source)
        {
            if (this.IsEnabled(EventLevel.Verbose, Keywords.PersistedDataAggregator))
            {
                this.EmptyResponseReceivedFromSource(source.Name);
            }
        }

        [Event(410, Level = EventLevel.Verbose, Keywords = Keywords.PersistedDataAggregator)]
        private void EmptyResponseReceivedFromSource(string source)
        {
            this.WriteEvent(410, source);
        }

        [SuppressMessage("Microsoft.Design", "CA1034:NestedTypesShouldNotBeVisible"),
         SuppressMessage("Microsoft.Design", "CA1053:StaticHolderTypesShouldNotHaveConstructors")]
        public class Keywords
        {
            public const EventKeywords PersistedDataAggregator = (EventKeywords)0x1;
        }
    }
}
