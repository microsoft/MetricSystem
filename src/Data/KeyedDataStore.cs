// ---------------------------------------------------------------------
// <copyright file="KeyedDataStore.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

// Collecting allocation stacks can aid in debugging resource leaks.
#undef COLLECT_STACKS

namespace MetricSystem.Data
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;

    using Microsoft.IO;
    using MetricSystem.Utilities;

    /// <summary>
    /// A storage handler for keyed data.
    /// </summary>
    /// <remarks>
    /// Supports the following varieties of operations:
    /// - Writing new individual keyed values.
    /// - Adding pairs of buffers containing keys and their associated data, potentially from a differing dimension set.
    /// - Merging of all buffered data into a single pair of converted buffers.
    /// - Serialization of merged data.
    /// - Querying of merged data.
    /// </remarks>
    internal sealed unsafe class KeyedDataStore<TInternal> :
        IEnumerable<KeyValuePair<Key, TInternal>>, IDisposable
        where TInternal : class, IInternalData, new()
    {
        // TODO: Consider making this configurable, for now we try to keep a relatively reasonable amount of
        // unmerged data before compressing/merging.
        private const int MaxUnmergedBufferSize = 1 << 23;

        private readonly bool multiValue;
        private readonly string allocationStack;
        private readonly RecyclableMemoryStreamManager memoryStreamManager;

        private readonly List<QueryableData> unmergedData = new List<QueryableData>();
        private bool dirty;
        private QueryableData mergedData;
        private QueryableSingleValueData pendingDataStream;
        private bool disposed;

        public KeyedDataStore(DimensionSet dimensionSet, RecyclableMemoryStreamManager memoryStreamManager, string sourceTag = "unknown")
            : this(dimensionSet, memoryStreamManager, null, 0, PersistedDataType.Unknown, sourceTag) { }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public KeyedDataStore(DimensionSet dimensionSet, RecyclableMemoryStreamManager memoryStreamManager,
                              MemoryStream initialData, int dataKeyCount, PersistedDataType initialDataType, string sourceTag = "unknown with data")
        {

            this.allocationStack =
#if COLLECT_STACKS
                sourceTag + " " + Environment.StackTrace;
#else
                string.Empty;
#endif

            this.DimensionSet = dimensionSet;
            this.memoryStreamManager = memoryStreamManager;

            if (initialData != null && dataKeyCount > 0)
            {
                this.mergedData = this.CreateDataFromStream(initialData, initialDataType, dataKeyCount);
            }
            else
            {
                this.mergedData = new QueryableSingleValueData(null, DimensionSet.Empty);
                if (initialData != null)
                {
                    initialData.Dispose();
                }
            }

            this.multiValue = (new TInternal()).MultiValue;
        }

        public DimensionSet DimensionSet { get; private set; }

        /// <summary>
        /// True if the store has been modified since construction. This will be true if there is any unmerged data, OR
        /// if data has been merged since the store was constructed.
        /// The bit may be set to false if merged data has been written out. Assuming no new unmerged data is added
        /// the data will then remain clean.
        /// </summary>
        public bool Dirty
        {
            get { return (this.HasUnmergedData || this.dirty); }
            set { this.dirty = value; }
        }

        /// <summary>
        /// Count of queryable keys stored (full data count may be higher).
        /// </summary>
        public int Count
        {
            get { return this.mergedData.Count; }
        }

        /// <summary>
        /// True if the store is entirely empty.
        /// </summary>
        public bool Empty
        {
            get { return !this.Dirty && this.Count == 0; }
        }

        /// <summary>
        /// Total size of serialized data. Note this may be an approximation.
        /// </summary>
        public long SerializedSize
        {
            get { return (this.HasUnmergedData ? 0 : this.mergedData.SerializedSize); }
        }

        public bool HasUnmergedData
        {
            get { return this.pendingDataStream != null || this.unmergedData.Count > 0; }
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (disposing && this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().ToString());
            }

            this.disposed = true;

            if (this.pendingDataStream != null)
            {
                this.pendingDataStream.Dispose();
                this.pendingDataStream = null;
            }

            foreach (var pair in this.unmergedData)
            {
                pair.Dispose();
            }
            this.unmergedData.Clear();

            if (this.mergedData != null)
            {
                this.mergedData.Dispose();
                this.mergedData = null;
            }
        }

        public IEnumerator<KeyValuePair<Key, TInternal>> GetEnumerator()
        {
            return this.mergedData.GetMatchingPairs(null).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        private QueryableData CreateDataFromStream(MemoryStream stream, PersistedDataType initialDataType, int keyCount)
        {
            var internalData = new TInternal();
            if (internalData.MultiValue)
            {
                return new QueryableMultiValueData(initialDataType, stream, this.DimensionSet, keyCount);
            }

            return new QueryableSingleValueData(stream, this.DimensionSet);
        }

        ~KeyedDataStore()
        {
            Events.Write.ObjectFinalized(this.GetType().ToString(), this.allocationStack);
            this.Dispose(false);
        }

        public void AddValue(DimensionSpecification dimensions, long value)
        {
            // We use stackalloc here because this method is used extremely frequently, and this saves a tremendous
            // amount of overhead in terms of short-lived garbage data.
            uint* keyData = stackalloc uint[this.DimensionSet.dimensions.Length];
            this.DimensionSet.PopulateKeyArray(dimensions, keyData);

            this.AddValue(keyData, value);
        }

        /// <summary>
        /// Writes the provided key/value pair to internal data.
        /// </summary>
        /// <param name="key">Data key.</param>
        /// <param name="value">Data value.</param>
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private void AddValue(uint* key, long value)
        {
            // If the single value buffer is either not there or full swap it for a new one.
            if (this.pendingDataStream == null || !this.pendingDataStream.Data.TryWrite(key, value))
            {
                lock (this)
                {
                    if (this.pendingDataStream == null || !this.pendingDataStream.Data.TryWrite(key, value))
                    {
                        var newData = new QueryableSingleValueData(this.memoryStreamManager.GetStream("WriteBuffer"),
                                                                   this.DimensionSet, true);
                        if (!newData.Data.TryWrite(key, value))
                        {
                            throw new InvalidOperationException("Unable to write data??");
                        }

                        this.SealWriteBuffer(newData);
                    }
                }
            }
        }

        private void AddData(QueryableData data)
        {
            this.unmergedData.Add(data);
            int totalUnmergedSize = 0;
            for (var i = 0; i < this.unmergedData.Count; ++i)
            {
                totalUnmergedSize += this.unmergedData[i].Size;
            }

            if (totalUnmergedSize >= MaxUnmergedBufferSize && this.unmergedData.Count > 1)
            {
                for (var i = 0; i < this.unmergedData.Count; ++i)
                {
                    this.unmergedData[i].PrepareForMerge(this.DimensionSet);
                }

                var merged = this.MergeBuffers(this.unmergedData);
                this.unmergedData.Add(merged);
            }
        }

        public void TakeData(KeyedDataStore<TInternal> otherData)
        {
            lock (this)
            {
                if (otherData.mergedData != null)
                {
                    this.unmergedData.Add(otherData.mergedData);
                    otherData.mergedData = null;
                }

                if (otherData.pendingDataStream != null)
                {
                    otherData.SealWriteBuffer(null);
                }

                this.unmergedData.AddRange(otherData.unmergedData);
                otherData.unmergedData.Clear();
            }
        }

        /// <summary>
        /// Merge all data into a single pair of ordered and compressed buffers. Assumes caller is no longer allowing
        /// individual value writes.
        /// </summary>
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public void Merge()
        {
            if (!this.HasUnmergedData)
            {
                return;
            }

            this.dirty = true;

            lock (this)
            {
                this.SealWriteBuffer(null);

                foreach (var pair in this.unmergedData)
                {
                    pair.PrepareForMerge(this.DimensionSet);
                }

                // Obviously this data was already merged and does not need a second preparation. We will clear out the
                // current list afterwards.
                this.unmergedData.Add(this.mergedData);

                this.mergedData = this.MergeBuffers(this.unmergedData);
            }
        }

        private QueryableData MergeBuffers(ICollection<QueryableData> buffers)
        {
            var mergedKeysBuffer = this.memoryStreamManager.GetStream("MergedKeys");
            var mergedValuesBuffer = this.multiValue ? this.memoryStreamManager.GetStream("MergedValues") : null;

            uint valueOffset = 0;
            var buffer = new byte[sizeof(uint)];

            // this ToArray call is bogus but otherwise we can't pass our list over for some reason.
            // TODO: figure this out. Not a huge deal as it's relatively low cost.
            foreach (var kvp in KeyedDataMerge<TInternal>.MergeSorted(buffers.ToArray()))
            {
                kvp.Key.Serialize(mergedKeysBuffer);
                var internalData = kvp.Value;
                if (this.multiValue)
                {
                    ByteConverter.FixedLengthEncoding.Encode(valueOffset, buffer, mergedKeysBuffer);
                    valueOffset += internalData.WriteToPersistedData(mergedValuesBuffer);
                }
                else
                {
                    internalData.WriteToPersistedData(mergedKeysBuffer);
                }
            }

            foreach (var pair in buffers)
            {
                pair.Dispose();
            }
            buffers.Clear();

            if (this.multiValue)
            {
                return new QueryableMultiValueData(mergedKeysBuffer, mergedValuesBuffer, this.DimensionSet);
            }
            else
            {
                return new QueryableSingleValueData(mergedKeysBuffer, this.DimensionSet);
            }
        }

        public void Serialize(Stream destinationStream)
        {
            if (this.HasUnmergedData)
            {
                throw new NotSupportedException("Cannot serialize un-merged data store.");
            }

            this.mergedData.Serialize(this.memoryStreamManager, destinationStream);
        }

        public bool Validate()
        {
            lock (this)
            {
                if (this.unmergedData.Any(data => !data.Validate()))
                {
                    return false;
                }

                return this.mergedData.Validate();
            }
        }

        public IEnumerable<KeyValuePair<Key, TInternal>> GetMatchingPairs(Key filter)
        {
            return this.mergedData.GetMatchingPairs(filter);
        }

        private void SealWriteBuffer(QueryableSingleValueData newData)
        {
            var currentData = this.pendingDataStream;
            this.pendingDataStream = newData;

            if (currentData != null)
            {
                currentData.Data.Seal();
                this.AddData(currentData);
            }
        }

        #region buffer manager types
        private abstract class QueryableData : IEnumerable<KeyValuePair<Key, IMergeSource>>, IDisposable
        {
            private readonly string allocationStack;
            private bool disposed;

            protected QueryableData(DimensionSet dimensionSet)
            {
            this.allocationStack =
#if COLLECT_STACKS
                Environment.StackTrace;
#else
                string.Empty;
#endif
                this.DimensionSet = dimensionSet;
            }

            public abstract int Count { get; }
            public abstract int Size { get; }
            public abstract long SerializedSize { get; }
            public DimensionSet DimensionSet { get; private set; }
            protected abstract void Dispose(bool disposing);
            public abstract IEnumerator<KeyValuePair<Key, IMergeSource>> GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }

            public abstract IEnumerable<KeyValuePair<Key, TInternal>> GetMatchingPairs(Key filterKey);
            public abstract void PrepareForMerge(DimensionSet targetDimensionSet);
            public abstract void Serialize(RecyclableMemoryStreamManager memoryManager, Stream destinationStream);
            public abstract bool Validate();

            public void Dispose()
            {
                if (this.disposed)
                {
                    throw new ObjectDisposedException(this.GetType().ToString());
                }

                this.disposed = true;
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }

            ~QueryableData()
            {
                Events.Write.ObjectFinalized(this.GetType().ToString(), this.allocationStack);
                this.Dispose(false);
            }
        }

        private sealed class QueryableMultiValueData : QueryableData
        {
            private MemoryStream valueStream;
            private MemoryStream keyStream;
            private BufferedKeyedData<uint> keys;
            private IBufferedValueArray values;

            public QueryableMultiValueData(PersistedDataType type, MemoryStream source, DimensionSet dimensionSet, int keyCount)
                : base(dimensionSet)
            {
                if (source.Length == 0)
                {
                    this.keys = new BufferedKeyedData<uint>(null, 0, 0, dimensionSet);
                    source.Dispose();
                }
                else
                {
                    this.keyStream = source;
                    this.valueStream = null;
                    var keyPortionLength = (int)BufferedKeyedData<uint>.GetBufferSizeForKeyCount(keyCount, dimensionSet);
                    var sourceBuffer = source.GetBuffer();
                    var sourceLength = (int)source.Length;

                    this.keys = new BufferedKeyedData<uint>(sourceBuffer, 0, keyPortionLength, dimensionSet);
                    this.values = BufferedValueArray.Create(type, sourceBuffer,
                                                                   keyPortionLength, sourceLength - keyPortionLength);
                }
            }

            public QueryableMultiValueData(MemoryStream keyStream, MemoryStream valueStream, DimensionSet dimensionSet)
                : base(dimensionSet)
            {
                if (keyStream.Length == 0)
                {
                    this.keys = new BufferedKeyedData<uint>(null, 0, 0, dimensionSet);
                    keyStream.Dispose();
                    valueStream.Dispose();
                }
                else
                {
                    this.keyStream = keyStream;
                    this.keys = new BufferedKeyedData<uint>(keyStream.GetBuffer(), 0, (int)keyStream.Length,
                                                            dimensionSet);

                    this.values = BufferedValueArray.Create(valueStream.GetBuffer(), 0, (int)valueStream.Length);
                    this.valueStream = valueStream;
                }
            }

            public override int Count
            {
                get { return this.keys.Count; }
            }

            public override int Size
            {
                get
                {
                    return (this.keyStream != null ? this.keyStream.Capacity : 0) +
                           (this.valueStream != null ? this.valueStream.Capacity : 0);
                }
            }

            public override long SerializedSize
            {
                get
                {
                    return (this.keyStream != null ? this.keyStream.Length : 0) +
                           (this.valueStream != null ? this.valueStream.Length : 0);
                }
            }

            protected override void Dispose(bool disposing)
            {
                if (this.keys != null)
                {
                    this.keys.Dispose();
                    this.keys = null;
                }
                if (this.keyStream != null)
                {
                    this.keyStream.Dispose();
                    this.keyStream = null;
                }

                if (this.values != null) // may be null for empty sets of keys.
                {
                    this.values.Dispose();
                    this.values = null;
                }
                if (this.valueStream != null)
                {
                    this.valueStream.Dispose();
                    this.valueStream = null;
                }
            }

            public override IEnumerator<KeyValuePair<Key, IMergeSource>> GetEnumerator()
            {
                foreach (var kvp in this.keys)
                {
                    yield return new KeyValuePair<Key, IMergeSource>(kvp.Key,
                                                                     new MultiValueMergeSource(this.values,
                                                                                                       kvp.Value));
                }
            }

            public override void PrepareForMerge(DimensionSet desiredDimensionSet)
            {
                this.keys.Convert(desiredDimensionSet);
                this.keys.Sort();
            }

            public override void Serialize(RecyclableMemoryStreamManager memoryStreamManager, Stream destinationStream)
            {
                if (this.values == null)
                {
                    this.keys.Serialize(destinationStream);
                }
                else
                {
                    if (this.values.CanWriteToPersistedData)
                    {
                        this.keys.Serialize(destinationStream);
                        this.values.Serialize(destinationStream);                        
                    }
                    else
                    {
                        // This will only happen in cases where we are re-serializing a legacy buffer to a new one
                        // which should be...never in production and only during some utility processing
                        this.TranscodeBuffers(memoryStreamManager, destinationStream);
                    }
                }
            }

            /// <summary>
            /// Helper to convert from legacy persisted data format to the current. The key array is a set of kvp's where the value
            /// is an offset in the value buffer. These all need to be recalculated with new value buffer VLE format
            /// </summary>
            private void TranscodeBuffers(RecyclableMemoryStreamManager memoryStreamManager, Stream destinationStream)
            {
                using (var intermediateValueStream = memoryStreamManager.GetStream("transcoding value-buffer"))
                {
                    uint currentOffset = 0;
                    var buffer = new byte[sizeof(uint)];
                    foreach (var kvp in this.keys)
                    {
                        // write the current key and then the offset in fixed length
                        kvp.Key.Serialize(destinationStream);
                        ByteConverter.FixedLengthEncoding.Encode(currentOffset, buffer, destinationStream);

                        var dict = new Dictionary<long, uint>();
                        var newEntries = this.values.ReadValuesInto(dict, kvp.Value);

                        currentOffset += BufferedValueArray.WriteVariableLengthEncodedDictionary(
                            dict, 
                            newEntries,
                            intermediateValueStream);
                    }

                    // now dump the values at the end of the key array
                    destinationStream.Write(intermediateValueStream.GetBuffer(), 0, (int)intermediateValueStream.Length);
                }
            }

            public override bool Validate()
            {
                if (!this.keys.Validate())
                {
                    return false;
                }

                foreach (var kvp in this.keys)
                {
                    if (!this.values.ValidateOffset(kvp.Value))
                    {
                        return false;
                    }
                }

                return true;
            }

            public override IEnumerable<KeyValuePair<Key, TInternal>> GetMatchingPairs(Key filter)
            {
                var valueData = new TInternal();
                foreach (var kvp in this.keys.GetMatchingPairs(filter))
                {
                    valueData.Clear();
                    valueData.MergeFrom(new MultiValueMergeSource(this.values, kvp.Value));
                    yield return new KeyValuePair<Key, TInternal>(kvp.Key, valueData);
                }
            }
        }

        private sealed class QueryableSingleValueData : QueryableData
        {
            private MemoryStream sourceStream;

            [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
            public QueryableSingleValueData(MemoryStream source, DimensionSet dimensionSet, bool writable = false)
                : base(dimensionSet)
            {
                this.sourceStream = source;
                if (source != null)
                {
                    this.Data = new BufferedKeyedData<long>(source.GetBuffer(), 0, (int)source.Length, dimensionSet,
                                                              writable);
                }
                else
                {
                    this.Data = new BufferedKeyedData<long>(null, 0, 0, dimensionSet);
                }
            }

            public BufferedKeyedData<long> Data { get; private set; }

            public override int Count
            {
                get { return this.Data.Count; }
            }

            public override int Size
            {
                get
                {
                    return this.sourceStream != null ? this.sourceStream.Capacity : 0;
                }
            }

            public override long SerializedSize
            {
                get
                {
                    return this.sourceStream != null ? this.sourceStream.Length : 0;
                }
            }

            public override IEnumerable<KeyValuePair<Key, TInternal>> GetMatchingPairs(Key filterKey)
            {
                var value = new TInternal();
                foreach (var kvp in this.Data.GetMatchingPairs(filterKey))
                {
                    value.Clear();
                    value.AddValue(kvp.Value);
                    yield return new KeyValuePair<Key, TInternal>(kvp.Key, value);
                }
            }

            public override void PrepareForMerge(DimensionSet targetDimensionSet)
            {
                this.Data.Convert(targetDimensionSet);
                this.Data.Sort();
            }

            public override void Serialize(RecyclableMemoryStreamManager unused, Stream destinationStream)
            {
                this.Data.Serialize(destinationStream);
            }

            public override bool Validate()
            {
                return this.Data.Validate();
            }

            protected override void Dispose(bool disposing)
            {
                if (this.Data != null)
                {
                    this.Data.Dispose();
                    this.Data = null;
                }
                if (this.sourceStream != null)
                {
                    this.sourceStream.Dispose();
                    this.sourceStream = null;
                }
            }

            public override IEnumerator<KeyValuePair<Key, IMergeSource>> GetEnumerator()
            {
                foreach (var kvp in this.Data)
                {
                    yield return
                        new KeyValuePair<Key, IMergeSource>(kvp.Key, new SingleValueMergeSource(kvp.Value));
                }
            }
        }
        #endregion
    }
}
