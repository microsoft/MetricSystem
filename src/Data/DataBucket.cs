// ---------------------------------------------------------------------
// <copyright file="DataBucket.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Threading;

    using Microsoft.IO;
    using MetricSystem.Utilities;

    internal sealed class DataBucket<TInternal>
        : IDisposable, IEquatable<DataBucket<TInternal>>
        where TInternal : class, IInternalData, new()
    {
        private const string FilenameTimestampFormat = "yyyyMMddHHmmssZ";
        private const string FilenameTimestampSeparator = "--";
        private const string FilenameSuffix = ".msdata";

        private const string NameFormat = "{0:" + FilenameTimestampFormat + "}" + FilenameTimestampSeparator +
                                          "{1:" + FilenameTimestampFormat + "}";

        private const string FilenameFormat = NameFormat + FilenameSuffix;

        private readonly Dictionary<string, PersistedDataSource> sources =
            new Dictionary<string, PersistedDataSource>(StringComparer.OrdinalIgnoreCase);

        private bool isDisposed = false;

        public DimensionSet DimensionSet { get; private set; }
        public string Filename { get; private set; }
        public string Blobname { get; private set; }
        private KeyedDataStore<TInternal> data; 
        private readonly RecyclableMemoryStreamManager memoryStreamManager;

        public DataBucket(DimensionSet dimensionSet, DateTime timestamp, long timeSpanInTicks, string storagePath,
                          RecyclableMemoryStreamManager memoryStreamManager)
        {
            timestamp = timestamp.ToUniversalTime();

            this.DimensionSet = dimensionSet;
            this.TimeSpan = TimeSpan.FromTicks(timeSpanInTicks);
            this.StartTime = RoundTimeStampToBucketKey(timestamp, timeSpanInTicks);
            this.memoryStreamManager = memoryStreamManager;

            if (storagePath != null)
            {
                this.Filename = Path.Combine(storagePath, GenerateFilename(this.StartTicks, this.EndTicks));
            }

            this.Blobname = GenerateBlobname(this.StartTicks, this.EndTicks);
            this.data = new KeyedDataStore<TInternal>(this.DimensionSet, this.memoryStreamManager, "db ctor");
        }

        /// <summary>
        /// </summary>
        /// <param name="dimensionSet"></param>
        /// <param name="fullFilenameOrBlobName">Can be a full filename or Blobname. Set the other param accordingly</param>
        /// <param name="memoryStreamManager"></param>
        /// <param name="storagePath">Null if fullFilename given, otherwise, carries a value</param>
        public DataBucket(DimensionSet dimensionSet, string fullFilenameOrBlobName,
                          RecyclableMemoryStreamManager memoryStreamManager, string storagePath)
        {
            DateTime start, end;
            if (storagePath == null)
            {
                if (!GetTimestampsFromFullFilename(fullFilenameOrBlobName, out start, out end))
                {
                    throw new ArgumentException("Invalid filename", "fullFilenameOrBlobName");
                }
                this.Filename = fullFilenameOrBlobName;
                this.Blobname = GetBlobnameFromFilename(fullFilenameOrBlobName);
            }
            else
            {
                if (!GetTimestampsFromFilename(fullFilenameOrBlobName, out start, out end))
                {
                    throw new ArgumentException("Invalid blobname", "fullFilenameOrBlobName");
                }
                this.Blobname = fullFilenameOrBlobName;
                this.Filename = GetFilenameFromBlobName(fullFilenameOrBlobName, storagePath);
            }

            this.DimensionSet = dimensionSet;
            this.memoryStreamManager = memoryStreamManager;

            this.StartTime = start;
            this.TimeSpan = (end - start);
            this.Sealed = false;
        }

        public DataBucket(IEnumerable<DataBucket<TInternal>> buckets, DimensionSet dimensionSet,
                          DateTime timestamp, long timeSpanInTicks, string storagePath,
                          RecyclableMemoryStreamManager memoryStreamManager)
            : this(dimensionSet, timestamp, timeSpanInTicks, storagePath, memoryStreamManager)
        {
            foreach (var dataBucket in buckets)
            {
                if (this.TimeSpan <= dataBucket.TimeSpan)
                {
                    Events.Write.AttemptToCompactIncompatibleQuanta(dataBucket.TimeSpan.Ticks, this.TimeSpan.Ticks);
                    throw new ArgumentException("Cannot compact bucket of equal or smaller quantum");
                }

                if (!dataBucket.Sealed)
                {
                    throw new ArgumentException("Attempted to compact data from unsealed bucket " + dataBucket);
                }

                this.UpdateFromDataBucket(dataBucket);
            }

            this.Seal();
        }

        public override string ToString()
        {
            return string.Format("{0} ({1}) - {2} ({3}) {4} {5} {6}", this.StartTicks, this.StartTime, this.EndTicks,
                this.EndTime, this.Filename, this.Sealed ? "sealed" : "unsealed",
                this.Dirty ? "dirty" : "clean");
        }

        /// <summary>
        /// Ticks in UTC.
        /// </summary>
        public long StartTicks
        {
            get { return this.StartTime.Ticks; }
        }

        /// <summary>
        /// Ticks in UTC.
        /// </summary>
        public long EndTicks
        {
            get { return this.StartTicks + this.TimeSpan.Ticks; }
        }

        /// <summary>
        /// Start time in UTC.
        /// </summary>
        public DateTime StartTime { get; private set; }

        /// <summary>
        /// End time in UTC.
        /// </summary>
        public DateTime EndTime
        {
            get { return new DateTime(this.EndTicks, DateTimeKind.Utc); }
        }

        /// <summary>
        /// Timespan for this bucket.
        /// </summary>
        public TimeSpan TimeSpan { get; private set; }

        /// <summary>
        /// Set if the data has been marked as sealed. If the data is sealed future attempts to update it will fail
        /// silently.
        /// </summary>
        public bool Sealed { get; private set; }

        /// <summary>
        /// Set if the data has been updated since being retrieved from backing store.
        /// </summary>
        public bool Dirty
        {
            get { return this.dirty || (this.data != null && this.data.Dirty); }
        }
        private bool dirty;

        /// <summary>
        /// True if the data is loaded in memory, false if it must be retrieved from persisted storage to be examined.
        /// </summary>
        public bool Loaded
        {
            get { return this.data != null; }
        }

        // internal data usage lock. Clients take a Read (shared) lock when accessing or modifying the data. 
        // Clients take a Write (exclusive) lock when clearing out the data
        private ReaderWriterLockSlim dataAccessLock = new ReaderWriterLockSlim();

        /// <summary>
        /// "Pin" the underlying data as "in-use" and cannot be freed until Unpin is called
        /// </summary>
        private void Pin()
        {
            this.dataAccessLock.EnterReadLock();

            try
            {
                // load the data if it is not already in memory
                this.Load();
            }
            catch
            {
                this.Unpin();
                throw;
            }
        }

        /// <summary>
        /// Release an earlier "pin" on underlying data is not in-use and can be freed as long as other Pin users are not using it
        /// </summary>
        private void Unpin()
        {
            this.dataAccessLock.ExitReadLock();
        }

        public void Dispose()
        {
            if (this.isDisposed)
            {
                throw new ObjectDisposedException("Double Disposed: " + this.ToString());
            }

            this.ReleaseData();
            this.isDisposed = true;
            this.dataAccessLock.Dispose();
            this.dataAccessLock = null;
        }

        private void Load()
        {
            if (this.data == null)
            {
                lock (this)
                {
                    if (this.data == null)
                    {
                        this.ReadFromStorage(this.Filename, false);
                    }
                }
            }
        }

        public bool Equals(DataBucket<TInternal> other)
        {
            if (other != null && other.StartTicks == this.StartTicks)
            {
                return true; // given usage elsewhere this really is all we need to check!
            }

            return false;
        }

        public static bool GetTimestampsFromFullFilename(string fullFilename, out DateTime start, out DateTime end)
        {
            if (!string.Equals(Path.GetExtension(fullFilename), FilenameSuffix, StringComparison.OrdinalIgnoreCase))
            {
                start = end = DateTime.MinValue;
                return false;
            }

            string filename = Path.GetFileNameWithoutExtension(fullFilename);
            if (!GetTimestampsFromFilename(filename, out start, out end))
            {
                return false;
            }
            return true;
        }

        public static bool GetTimestampsFromFilename(string filename, out DateTime start, out DateTime end)
        {
            start = end = DateTime.MinValue;

            string[] timestamps = filename.Split(new[] {FilenameTimestampSeparator}, StringSplitOptions.None);
            if (timestamps.Length != 2)
            {
                return false;
            }

            if (!DateTime.TryParseExact(timestamps[0], FilenameTimestampFormat, CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal, out start))
            {
                return false;
            }
            if (!DateTime.TryParseExact(timestamps[1], FilenameTimestampFormat, CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal, out end))
            {
                return false;
            }

            return true;
        }

        public static DateTime RoundTimeStampToBucketKey(DateTime timestamp, long bucketTimeSpanInTicks)
        {
            return new DateTime(timestamp.Ticks - (timestamp.Ticks % bucketTimeSpanInTicks),
                                DateTimeKind.Utc);
        }

        public bool HasDataForTimestamp(DateTime timestamp)
        {
            return (this.StartTicks <= timestamp.Ticks && this.EndTicks > timestamp.Ticks);
        }

        public PersistedDataSource AddDataSource(string dataSourceName)
        {
            lock (this.sources)
            {
                PersistedDataSource source;
                if (!this.sources.TryGetValue(dataSourceName, out source))
                {
                    source = new PersistedDataSource(dataSourceName, PersistedDataSourceStatus.Unknown);
                    this.sources.Add(source.Name, source);
                    this.dirty = true;
                }

                return source;
            }
        }

        public void SetSourceUnavailable(string sourceName)
        {
            if (this.Sealed)
            {
                throw new InvalidOperationException("Cannot update data source in sealed data.");
            }

            var source = this.FindSource(sourceName);
            // expected to never be null, okay to allow throw. also okay to update source status outside any locks.
            source.Status = PersistedDataSourceStatus.Unavailable;
            this.dirty = true;
        }

        public void SetSourceAvailable(string sourceName)
        {
            if (this.Sealed)
            {
                throw new InvalidOperationException("Cannot update data source in sealed data.");
            }

            var source = this.AddDataSource(sourceName);
            source.Status = PersistedDataSourceStatus.Available;
            this.dirty = true;
        }

        public IList<string> GetPendingSources()
        {
            lock (this.sources)
            {
                return (from s in this.sources.Values
                        where s.Status == PersistedDataSourceStatus.Unknown
                        select s.Name).ToList();
            }
        }

        public void UpdateDataFromSources(IList<string> sourceList, DimensionSet sourceDimensions,
                                          KeyedDataStore<TInternal> sourceData)
        {
            if (this.Sealed)
            {
                throw new InvalidOperationException("Attempt to write to sealed bucket.");
            }

            foreach (var s in sourceList)
            {
                // Below we do some sanity checking to make sure that we're not ingesting data we were already given,
                // or ingesting data from a source that wasn't pre-declared as an input. Either of these would indicate
                // an upstream logic fault.
                var source = this.FindSource(s);
                if (source == null)
                {
                    throw new InvalidOperationException("Adding data from previously unknown source " + s);
                }

                if (source.Status != PersistedDataSourceStatus.Unknown)
                {
                    throw new InvalidOperationException("Double adding data from source " + s);
                }

                source.Status = PersistedDataSourceStatus.Available;
            }

            using(SharedLock.OpenExclusive(this.dataAccessLock))
            {
                this.Load();
                if (this.data == null || this.data.Empty)
                {
                    this.DimensionSet = sourceDimensions;
                    this.data = sourceData;
                }
                else
                {
                    this.data.TakeData(sourceData);
                    sourceData.Dispose();
                }

                this.dirty = true;
            }
        }

        private PersistedDataSource FindSource(string sourceName)
        {
            lock (this.sources)
            {
                PersistedDataSource source;
                if (this.sources.TryGetValue(sourceName, out source))
                {
                    return source;
                }
            }

            return null;
        }

        public void AddValue(DimensionSpecification dims, long value)
        {
            if (this.Sealed)
            {
                throw new InvalidOperationException("Attempt to write to sealed bucket.");
            }

            this.Pin();
            try
            {
                this.data.AddValue(dims, value);
            }
            finally
            {
                this.Unpin();
            }
        }

        /// <summary>
        /// Represents a match result from the GetMatches calls.
        /// </summary>
        public sealed class MatchResult
        {
            /// <summary>
            /// Dimension values provided for this match result. Updated for split/cross queries to specify the value
            /// used for the split dimension.
            /// </summary>
            public DimensionSpecification DimensionValues;

            /// <summary>
            /// Combined data for the matched value. May be null if there was no match.
            /// </summary>
            public TInternal Data;

            /// <summary>
            /// Count of distinct matched elements which combined for the matched data.
            /// </summary>
            public int DataCount;

            /// <summary>
            /// Value of the dimension used for splitting, if any.
            /// </summary>
            public string SplitDimensionValue;

            internal MatchResult(DimensionSpecification dims, string splitValue)
            {
                this.DimensionValues = new DimensionSpecification(dims);
                this.SplitDimensionValue = splitValue;
                this.Data = null;
                this.DataCount = 0;
            }

            internal void UpdateData(TInternal data)
            {
                ++this.DataCount;
                if (this.Data == null)
                {
                    this.Data = new TInternal();
                }
                this.Data.MergeFrom(data);
            }
        }

        /// <summary>
        /// Get all values matching the provided dimensions.
        /// </summary>
        /// <param name="dims">Dimensions to filter with. Not all dimensions are required.</param>
        /// <returns>An enumeration of resulting matches. The enumeration will always be one item long.</returns>
        public IEnumerable<MatchResult> GetMatches(DimensionSpecification dims)
        {
            this.Pin();
            try
            {
                var key = this.DimensionSet.CreateKey(dims);

                // If we have no data simply provide an empty match.
                if (this.data == null || this.data.Count == 0)
                {
                    return new[] {new MatchResult(dims, string.Empty) {Data = null,}};
                }
                else
                {
                    var result = new MatchResult(dims, string.Empty);
                    foreach (var kvp in this.data.GetMatchingPairs(key))
                    {
                        result.UpdateData(kvp.Value);
                    }

                    return new[] {result};
                }
            }
            finally
            {
                this.Unpin();
            }
        }

        /// <summary>
        /// Get all values matching the provided dimensions, split by the given dimension key (e.g. cross query).
        /// </summary>
        /// <param name="dims">Dimensions to filter with. Not all dimensions are required.</param>
        /// <param name="splitDimensionKey">The dimension to use for split/cross querying.</param>
        /// <returns>An enumeration of resulting matches.</returns>
        public IEnumerable<MatchResult> GetMatchesSplitByDimension(DimensionSpecification dims,
            string splitDimensionKey)
        {
            this.Pin();

            try
            {
                var offset = this.DimensionSet.GetOffsetOfDimension(splitDimensionKey);
                if (offset < 0)
                {
                    throw new KeyNotFoundException(splitDimensionKey);
                }

                // Filter for matching dimensions though matching the split-by-dimension as a wildcard (retrieve all).
                var splitDimension = this.DimensionSet.dimensions[offset];
                var key = this.DimensionSet.CreateKey(dims);
                key.Values[offset] = Key.WildcardDimensionValue;

                var allMatches = this.data.GetMatchingPairs(key);

                // now make a single pass over the tree to sort the results by the splitKey dimension
                var splitByDimensionValues = new Dictionary<uint, MatchResult>();

                // note: this forcibly enumerates the entire key tree (as opposed to lazily enumerating as needed by the caller of this 
                // API. Too bad. We need to sort into buckets one-pass versus making N bucketing passes. 
                foreach (var match in allMatches)
                {
                    MatchResult result;
                    var splitByKeyValue = match.Key.Values[offset];
                    if (!splitByDimensionValues.TryGetValue(splitByKeyValue, out result))
                    {
                        var splitKey = splitDimension.IndexToString(match.Key.Values[offset]);
                        result = new MatchResult(dims, splitDimension.IndexToString(match.Key.Values[offset]));
                        result.DimensionValues[splitDimensionKey] = splitKey;
                        splitByDimensionValues.Add(splitByKeyValue, result);
                    }

                    result.UpdateData(match.Value);
                }

                return splitByDimensionValues.Values;
            }
            finally
            {
                this.Unpin();
            }
        }

        /// <summary>
        /// Provides all matching values for a named dimension. If the dimension is unknown no data will be returned.
        /// </summary>
        /// <param name="dimensionName">The dimension.</param>
        /// <param name="filterDims">Dimensions to filter by.</param>
        /// <returns>An enumeration of all known values. Values may be repeated.</returns>
        public IEnumerable<string> GetDimensionValues(string dimensionName, DimensionSpecification filterDims)
        {
            var offset = this.DimensionSet.GetOffsetOfDimension(dimensionName);
            if (offset < 0)
            {
                yield break;
            }

            this.Pin();

            try
            {
                Key filter = this.DimensionSet.CreateKey(filterDims);
                foreach (var kvp in this.data.GetMatchingPairs(filter))
                {
                    yield return this.DimensionSet.GetDimensionValueAtOffset(kvp.Key, offset);
                }
            }
            finally
            {
                this.Unpin();
            }
        }

        public override int GetHashCode()
        {
            // This will safely give us a 32bit number with no loss for quite a while.
            return (int)(this.StartTicks / TimeSpan.TicksPerMinute);
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as DataBucket<TInternal>);
        }

        /// <summary>
        /// Force load of persisted data.
        /// </summary>
        public void LoadData()
        {
            this.Pin();
            this.Unpin();
        }

        /// <summary>
        /// Load only the source data for the bucket.
        /// </summary>
        public void LoadSources()
        {
            // XXX: not in love with this design, but it's a quick way to ensure we know about sources at load.
            this.ReadFromStorage(this.Filename, true);
        }

        /// <summary>
        /// Release data from memory and write it to persisted storage if possible. This call will block until all users
        /// are finished accessing the data.
        /// </summary>
        public void ReleaseData()
        {
            using (SharedLock.OpenExclusive(this.dataAccessLock))
            {
                this.Persist();
                if (this.data != null)
                {
                    this.data.Dispose();
                    // force unloading our (probably populous) dimension set by doing a shallow copy.
                    // NOTE: This also gets rid of potentially large arrays whereas clearing them might not do so. Other
                    // work could be done to free up this memory but there's not a ton of value in this.
                    this.DimensionSet = new DimensionSet(this.DimensionSet);
                }

                // Once sealed we can guarantee no more source updates.
                if (this.Sealed)
                {
                    this.sources.Clear();
                }

                this.data = null;
            }
        }

        /// <summary>
        /// Seal data (preventing further modification). Does not write to permanent storage.
        /// </summary>
        public void Seal()
        {
            using (SharedLock.OpenExclusive(this.dataAccessLock))
            {
                this.Flush();
                this.Sealed = true;
            }
        }

        /// <summary>
        /// Flushes any updates in the underlying data (merges in any pending data).
        /// </summary>
        public void Flush()
        {
            if (this.data != null)
            {
                this.data.Merge();
            }
        }

        /// <summary>
        /// Write data to persisted storage if possible.
        /// </summary>
        public void Persist()
        {
            this.WriteToFile();
        }

        /// <summary>
        /// Permanently delete data.
        /// </summary>
        public void PermanentDelete()
        {
            if (File.Exists(this.Filename))
            {
                this.LocalStorageDelete();
            }
        }

        /// <summary>
        /// Delete persisted storage data.
        /// </summary>
        public void LocalStorageDelete()
        {
            if (this.Filename != null)
            {
                Events.Write.DeletingDataBucketFromLocalStorage(this.Filename);
                try
                {
                    File.Delete(this.Filename);
                    return;
                }
                catch (FileNotFoundException) { }
                catch (DirectoryNotFoundException) { }
                catch (IOException) { }

                Events.Write.FailedToDeleteDataBucketFromLocalStorage(this.Filename);
            }
        }

        public bool Serialize(string name, Stream destination)
        {
            if (!this.Sealed)
            {
                return false;
            }

            // If we already have data on the disk stream if from there instead of paying re-serialization cost.
            // We prefer to do this even if the data is actually loaded.
            if (this.Filename != null && !this.Dirty)
            {
                try
                {
                    var bytes = new byte[8192];
                    using (var fs = new FileStream(this.Filename, FileMode.Open, FileAccess.Read))
                    {
                        int readBytes;
                        while ((readBytes = fs.Read(bytes, 0, bytes.Length)) != 0)
                        {
                            destination.Write(bytes, 0, readBytes);
                        }
                    }

                    return true;
                }
                catch (FileNotFoundException)
                {
                    return false;
                }
            }

            this.Pin();
            try
            {
                using (var writer = new PersistedDataWriter(destination, this.DimensionSet, this.memoryStreamManager))
                {
                    writer.WriteData(name, this.StartTime, this.EndTime, (uint)this.data.Count, this.sources.Values,
                                     this.data);
                }

                return true;
            }
            finally
            {
                this.Unpin();
            }
        }

        public void UpdateFromDataBucket(DataBucket<TInternal> otherBucket)
        {
            // If the other bucket isn't loaded and is file backed we can just read their file data (this helps
            // to avoid creating additional unused data)
            // We don't need to lock anything in that case either since we are guaranteed a sealed data bucket
            // and can infer its file contents will not change.

            otherBucket.Pin();
            try
            {
                // Note that we don't mark the other bucket as dirty, so it should be reasonable to simply take
                // its data once it's loaded (and then allow it to unload).
                this.data.TakeData(otherBucket.data);
                otherBucket.data.Dispose();
                otherBucket.data = null;
                this.MergeSourceStatus(otherBucket.sources.Values);
            }
            finally
            {
                otherBucket.Unpin();
            }
        }

        [SuppressMessage("Microsoft.Usage",
            "CA2202:Do not dispose objects multiple times",
            Justification = "PersistedDataWriter does not Dispose input stream.")]
        private void WriteToFile()
        {
            lock (this)
            {
                if (this.Filename == null || !this.Dirty || this.data == null)
                {
                    return;
                }

                this.Flush();

                Events.Write.BeginWritingData(this.Filename);
                using (var stream = new FileStream(this.Filename, FileMode.Create, FileAccess.Write, FileShare.Read))
                using (var writer = new PersistedDataWriter(stream, this.DimensionSet, this.memoryStreamManager))
                {
                    writer.WriteData(string.Empty, this.StartTime, this.EndTime, (uint)this.data.Count,
                                     this.sources.Values, this.data);
                }

                this.dirty = false;
                this.data.Dirty = false;
                Events.Write.EndWritingData(this.Filename);
            }
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public Stream WriteToMemoryStream()
        {
            lock (this)
            {
                if (this.data == null)
                {
                    return null;
                }
                var stream = new MemoryStream();

                using (var writer = new PersistedDataWriter(stream, this.DimensionSet, this.memoryStreamManager))
                {
                    writer.WriteData(string.Empty, this.StartTime, this.EndTime, (uint)this.data.Count,
                                     this.sources.Values, this.data);

                }
                stream.Flush();
                stream.Position = 0;
                return stream;
            }
        }

        [SuppressMessage("Microsoft.Usage",
            "CA2202:Do not dispose objects multiple times",
            Justification = "PersistedDataReader does not Dispose input stream.")]
        private void ReadFromStorage(string filename, bool sourcesOnly)
        {
            KeyedDataStore<TInternal> readData = null;
            try
            {
                Events.Write.BeginLoadingData(filename);
                using (var stream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    using (var reader = new PersistedDataReader(stream, this.memoryStreamManager, this.DimensionSet))
                    {
                        // We'll only ever write one set of data, so we only try to read one.
                        if (reader.ReadDataHeader())
                        {
                            this.MergeSourceStatus(reader.Header.Sources);

                            if (sourcesOnly)
                            {
                                return;
                            }

                            if (typeof(TInternal) != reader.DataType)
                            {
                                throw new InvalidDataException(
                                    string.Format(
                                                  "this.start={0},this.end={1}, hdr.start={2}, hdr.end={3}, this.type={4}, hdr.type={5}",
                                                  this.StartTicks, this.EndTicks, reader.StartTime.Ticks,
                                                  reader.EndTime.Ticks,
                                                  typeof(TInternal), reader.DataType));
                            }

                            readData = reader.LoadData<TInternal>();
                            if (this.DimensionSet.Equals(reader.DimensionSet) &&
                                (this.data == null || this.data.Empty))
                            {
                                this.DimensionSet = reader.DimensionSet;
                                if (this.data != null)
                                {
                                    this.data.Dispose();
                                }
                                this.data = readData;
                                readData = null;
                            }
                            else
                            {
                                if (this.data == null)
                                {
                                    this.data = new KeyedDataStore<TInternal>(this.DimensionSet,
                                                                                             this.memoryStreamManager,
                                                                                             "read data");
                                }
                                this.data.TakeData(readData);
                                this.data.Merge();
                            }
                        }
                    }
                }
            }
            catch (FileNotFoundException)
            {
                Events.Write.SealedDataFileMissing(filename);
            }
            catch (PersistedDataException ex)
            {
                Events.Write.PersistedDataException(ex);
                Events.Write.DiscardingIncompleteData(filename);
                if (this.data != null)
                {
                    this.data.Dispose();
                }
                this.data = null;
            }
            catch (OutOfMemoryException)
            {
                // TODO: this code is here to deal with file corruption issues, once checksums have been added and
                // are fully available it must be removed.
                File.Delete(filename);
                throw;
            }
            finally
            {
                if (readData != null)
                {
                    readData.Dispose();
                }
            }

            // if data is still null, some error happened loading the file (empty, missing, corrupt). 
            // Just start over with clean data. 
            if (this.data == null)
            {
                if (File.Exists(this.Filename))
                {
                    File.Delete(this.Filename);
                }

                this.data = new KeyedDataStore<TInternal>(this.DimensionSet, this.memoryStreamManager,
                                                                         "placeholder for invalid data.");
            }

            Events.Write.EndLoadingData(filename);
        }

        private void MergeSourceStatus(IEnumerable<PersistedDataSource> otherSources)
        {
            lock (this.sources)
            {
                foreach (var otherSource in otherSources)
                {
                    var mySource = this.FindSource(otherSource.Name);
                    if (mySource == null)
                    {
                        this.sources.Add(otherSource.Name, new PersistedDataSource(otherSource.Name, otherSource.Status));
                    }
                    else
                    {
                        mySource.Status = mySource.Status.Combine(otherSource.Status);
                    }
                }
            }
        }

        private static string GenerateFilename(long startTicks, long endTicks)
        {
            var start = new DateTime(startTicks, DateTimeKind.Utc);
            var end = new DateTime(endTicks, DateTimeKind.Utc);

            return string.Format(FilenameFormat, start, end);
        }
        private static string GenerateBlobname(long startTicks, long endTicks)
        {
            var start = new DateTime(startTicks, DateTimeKind.Utc);
            var end = new DateTime(endTicks, DateTimeKind.Utc);

            return string.Format(NameFormat, start, end);
        }

        private static string GetBlobnameFromFilename(string filename)
        {
            return Path.GetFileNameWithoutExtension(filename);
        }

        public static string GetFilenameFromBlobName(string blobname, string storagePath)
        {
            return Path.Combine(storagePath, blobname, FilenameSuffix);
        }
    }
}
