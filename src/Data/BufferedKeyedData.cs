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

namespace MetricSystem.Data
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;

    /// <summary>
    /// Provides a wrapper for a group of <see cref="Key"/>s with associated value data.
    /// </summary>
    /// <typeparam name="TValue">Type of value data to store (must be a 2/4/8 byte integral type).</typeparam>
    /// <remarks>
    /// Once created the collection takes ownership of its source stream. It must be properly disposed.
    /// The following operations can be performed "in-place" on a collection:
    /// - Sort of keys (any number of times).
    /// - Conversion to a new <see cref="DimensionSet"/> after being created (once).
    /// - Enumeration (any number of times).
    /// </remarks>
    internal sealed unsafe class BufferedKeyedData<TValue> : BufferedData<TValue>, IEnumerable<KeyValuePair<Key, long>>
    {
        private const int UnmappedDimension = -1;

        private readonly int entrySizeBytes;
        private readonly int keyLength;
        private readonly int maxCount;
        private int[] conversionMap;
        private bool converted;
        private int currentCount;

        private DimensionSet dimensionSet;

        private bool writable;

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="sourceBuffer">Source data buffer (may be null)</param>
        /// <param name="offset">Offset into source buffer.</param>
        /// <param name="initialDataLength">Initial length of data in source buffer.</param>
        /// <param name="dimensionSet">DimensionSet for source keys.</param>
        /// <param name="writable">Whether the data is writable. If the data is writable the initial data length must be 0.</param>
        public BufferedKeyedData(byte[] sourceBuffer, int offset, int initialDataLength, DimensionSet dimensionSet,
                                 bool writable = false)
            : base(sourceBuffer, offset, initialDataLength)
        {
            this.DimensionSet = dimensionSet;
            this.keyLength = this.DimensionSet.dimensions.Length;

            this.entrySizeBytes = (this.keyLength * sizeof(uint)) + FixedLengthValueSize;

            // null buffer is ok (means empty set)
            if (writable)
            {
                if (initialDataLength != 0 || this.DataBuffer == null)
                {
                    throw new ArgumentException("Must provide valid and empty buffer to write.");
                }

                this.writable = true;
                this.maxCount = this.BufferLength / this.entrySizeBytes;
                if (this.maxCount == 0)
                {
                    throw new ArgumentException("Provided buffer is not large enough to hold any values.");
                }

                this.currentCount = 0;
            }
            else
            {
                if (this.InitialDataLength % this.entrySizeBytes != 0)
                {
                    throw new ArgumentException(
                        "Provided buffer is not correctly sized and may be truncated or invalid.");
                }

                this.maxCount = this.InitialDataLength / this.entrySizeBytes;
                this.currentCount = this.maxCount;
            }
        }

        internal DimensionSet DimensionSet
        {
            get { return this.dimensionSet; }
            private set
            {
                if (value != null)
                {
                    this.WildcardKey = Key.GetWildcardKey(value);
                }
                this.dimensionSet = value;
            }
        }

        protected override int ValidDataLength
        {
            get { return this.currentCount * this.entrySizeBytes; }
        }

        /// <summary>
        /// Wildcard (match-all) key for this group.
        /// </summary>
        public Key WildcardKey { get; private set; }

        /// <summary>
        /// Number of key pointers within this collection.
        /// </summary>
        public int Count
        {
            get { return this.currentCount; }
        }

        /// <summary>
        /// Helper to get the address of data at the specified index.
        /// </summary>
        private byte* this[int idx]
        {
            get { return this.DataBuffer + (idx * this.entrySizeBytes); }
        }

        public static long GetBufferSizeForKeyCount(long keyCount, DimensionSet dimensionSet)
        {
            return keyCount * (dimensionSet.dimensions.Length * sizeof(uint) + FixedLengthValueSize);
        }

        public static long GetKeyCountForBufferSize(long bufferSize, DimensionSet dimensionSet)
        {
            return bufferSize / (dimensionSet.dimensions.Length * sizeof(uint) + FixedLengthValueSize);
        }

        /// <summary>
        /// Attempt to write a key with specified data (pointer value).
        /// </summary>
        /// <param name="key">Key to write (must be of known key length).</param>
        /// <param name="value">Pointer data to write.</param>
        /// <returns>True if a value could be written. If false the buffer is full.</returns>
        public bool TryWrite(uint* key, TValue value)
        {
            this.CheckDisposed();

            if (!this.writable || this.currentCount > this.maxCount)
            {
                return false;
            }

            var count = Interlocked.Increment(ref this.currentCount);
            if (count > this.maxCount)
            {
                Interlocked.Decrement(ref this.currentCount);
                return false;
            }

            var start = this[count - 1];
            for (var i = 0; i < this.keyLength; ++i)
            {
                *((uint *)start + i) = key[i];
            }
            WriteFixedLengthValueToBuffer(value, start + (this.keyLength * sizeof(uint)));

            return true;
        }

        public void Seal()
        {
            this.CheckDisposed();

            if (!this.writable)
            {
                throw new NotSupportedException("Cannot seal buffer multiple times.");
            }

            this.writable = false;
        }

        /// <summary>
        /// Convert the current key pointers to a new dimension set. This operation may only be performed one time on
        /// a set.
        /// </summary>
        /// <param name="newSet">New set to convert to.</param>
        public void Convert(DimensionSet newSet)
        {
            this.CheckDisposed();
            if (this.converted)
            {
                throw new NotSupportedException("Conversion may only be performed once.");
            }

            if (this.writable)
            {
                throw new NotSupportedException("May not convert writable set.");
            }

            this.converted = true;
            if (ReferenceEquals(newSet, this.DimensionSet))
            {
                return;
            }

            // Build a slot-map for our source dimensions to the new dimension set's. 
            this.conversionMap = new int[this.DimensionSet.dimensions.Length];
            for (var d = 0; d < this.DimensionSet.dimensions.Length; ++d)
            {
                this.conversionMap[d] = UnmappedDimension;
                for (var n = 0; n < newSet.dimensions.Length; ++n)
                {
                    if (this.DimensionSet.dimensions[d].Equals(newSet.dimensions[n]))
                    {
                        this.conversionMap[d] = n;
                        break;
                    }
                }
            }

            for (var k = 0; k < this.Count; ++k)
            {
                var keyData = (uint*)this[k];
                for (var i = 0; i < this.keyLength; ++i)
                {
                    if (this.conversionMap[i] == UnmappedDimension)
                    {
                        keyData[i] = Key.WildcardDimensionValue;
                    }
                    else
                    {
                        var currentValue = this.DimensionSet.dimensions[i].IndexToString(keyData[i]);
                        keyData[i] = newSet.dimensions[this.conversionMap[i]].StringToIndex(currentValue);
                    }
                }
            }

            this.DimensionSet = newSet;
        }

        // bottom-up merge sort combined with an in-place merge algorithm for O(1) memory use 
        public void Sort()
        {
            this.CheckDisposed();

            if (this.writable)
            {
                throw new NotSupportedException("May not sort writable set.");
            }

            this.UnsafeSort();
        }

        public bool Validate()
        {
            this.CheckDisposed();

            var key = new Key(new uint[this.keyLength]);
            for (var i = 0; i < this.Count; ++i)
            {
                for (var k = 0; k < this.keyLength; ++k)
                {
                    key[k] = *((uint*)this[i] + k);
                }

                if (!this.dimensionSet.Validate(key))
                {
                    return false;
                }
            }

            return true;
        }

        #region BlockSort implementation
        // BlockSort implementation via:
        // http://en.wikipedia.org/wiki/Block_sort
        // Performs an in-place sort in at worst O(n log n) complexity.
        // Note: does not implement caching code from 'WikiSort' public domain reference code at this time.
        // Suppressing FxCop warnings to keep this code more closely comparable with its source WikiSort implementation.
        [SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity"),
         SuppressMessage("Microsoft.Maintainability", "CA1505:AvoidUnmaintainableCode")]
        private void UnsafeSort()
        {
            if (this.Count < 4)
            {
                switch (this.Count)
                {
                case 3:
                    if (Key.CompareSized(this[1], this[0], this.keyLength) < 0)
                    {
                        this.SwapEntries(this[1], this[0]);
                    }

                    if (Key.CompareSized(this[2], this[1], this.keyLength) < 0)
                    {
                        this.SwapEntries(this[2], this[1]);
                        if (Key.CompareSized(this[1], this[0], this.keyLength) < 0)
                        {
                            this.SwapEntries(this[1], this[0]);
                        }
                    }
                    break;
                case 2:
                    if (Key.CompareSized(this[1], this[0], this.keyLength) < 0)
                    {
                        this.SwapEntries(this[1], this[0]);
                    }
                    break;
                }

                return;
            }

            var iterator = new Iterator(this.Count, 4);
            while (!iterator.Finished())
            {
                var order = new[] {0, 1, 2, 3, 4, 5, 6, 7};
                var range = iterator.Next();

                switch (range.Length)
                {
                case 8:
                    this.NetSwap(order, range, 0, 1);
                    this.NetSwap(order, range, 2, 3);
                    this.NetSwap(order, range, 4, 5);
                    this.NetSwap(order, range, 6, 7);
                    this.NetSwap(order, range, 0, 2);
                    this.NetSwap(order, range, 1, 3);
                    this.NetSwap(order, range, 4, 6);
                    this.NetSwap(order, range, 5, 7);
                    this.NetSwap(order, range, 1, 2);
                    this.NetSwap(order, range, 5, 6);
                    this.NetSwap(order, range, 0, 4);
                    this.NetSwap(order, range, 3, 7);
                    this.NetSwap(order, range, 1, 5);
                    this.NetSwap(order, range, 2, 6);
                    this.NetSwap(order, range, 1, 4);
                    this.NetSwap(order, range, 3, 6);
                    this.NetSwap(order, range, 2, 4);
                    this.NetSwap(order, range, 3, 5);
                    this.NetSwap(order, range, 3, 4);
                    break;
                case 7:
                    this.NetSwap(order, range, 1, 2);
                    this.NetSwap(order, range, 3, 4);
                    this.NetSwap(order, range, 5, 6);
                    this.NetSwap(order, range, 0, 2);
                    this.NetSwap(order, range, 3, 5);
                    this.NetSwap(order, range, 4, 6);
                    this.NetSwap(order, range, 0, 1);
                    this.NetSwap(order, range, 4, 5);
                    this.NetSwap(order, range, 2, 6);
                    this.NetSwap(order, range, 0, 4);
                    this.NetSwap(order, range, 1, 5);
                    this.NetSwap(order, range, 0, 3);
                    this.NetSwap(order, range, 2, 5);
                    this.NetSwap(order, range, 1, 3);
                    this.NetSwap(order, range, 2, 4);
                    this.NetSwap(order, range, 2, 3);
                    break;
                case 6:
                    this.NetSwap(order, range, 1, 2);
                    this.NetSwap(order, range, 4, 5);
                    this.NetSwap(order, range, 0, 2);
                    this.NetSwap(order, range, 3, 5);
                    this.NetSwap(order, range, 0, 1);
                    this.NetSwap(order, range, 3, 4);
                    this.NetSwap(order, range, 2, 5);
                    this.NetSwap(order, range, 0, 3);
                    this.NetSwap(order, range, 1, 4);
                    this.NetSwap(order, range, 2, 4);
                    this.NetSwap(order, range, 1, 3);
                    this.NetSwap(order, range, 2, 3);
                    break;
                case 5:
                    this.NetSwap(order, range, 0, 1);
                    this.NetSwap(order, range, 3, 4);
                    this.NetSwap(order, range, 2, 4);
                    this.NetSwap(order, range, 2, 3);
                    this.NetSwap(order, range, 1, 4);
                    this.NetSwap(order, range, 0, 3);
                    this.NetSwap(order, range, 0, 2);
                    this.NetSwap(order, range, 1, 3);
                    this.NetSwap(order, range, 1, 2);
                    break;
                case 4:
                    this.NetSwap(order, range, 0, 1);
                    this.NetSwap(order, range, 2, 3);
                    this.NetSwap(order, range, 0, 2);
                    this.NetSwap(order, range, 1, 3);
                    this.NetSwap(order, range, 1, 2);
                    break;
                }
            }
            if (this.Count < 8)
            {
                return;
            }

            var pull = new Pull[2];
            pull[0] = new Pull();
            pull[1] = new Pull();

            while (true)
            {
                // this is where the in-place merge logic starts! 
                // 1. pull out two internal buffers each containing vA unique values 
                //     1a. adjust blockSize and buffer_size if we couldn't find enough unique values 
                // 2. loop over the A and B subarrays within this level of the merge sort 
                //     3. break A and B into blocks of size 'blockSize' 
                //     4. "tag" each of the A blocks with values from the first internal buffer 
                //     5. roll the A blocks through the B blocks and drop/rotate them where they belong 
                //     6. merge each A block with any B values that follow, using the cache or the second internal buffer 
                // 7. sort the second internal buffer if it exists 
                // 8. redistribute the two internal buffers back into the array 

                var blockSize = (int)Math.Sqrt(iterator.Length);
                var bufferSize = (iterator.Length / blockSize + 1);

                // as an optimization, we really only need to pull out the internal buffers once for each level of merges 
                // after that we can reuse the same buffers over and over, then redistribute it when we're finished with this level 
                int count;
                int index;
                var pullIndex = 0;

                var buffer1 = new Range(0, 0);
                var buffer2 = new Range(0, 0);
                pull[0] = new Pull();
                pull[1] = new Pull();

                // find two internal buffers of size 'buffer_size' each 
                var find = bufferSize + bufferSize;
                var findSeparately = false;

                if (find > iterator.Length)
                {
                    // we can't fit both buffers into the same A or B subarray, so find two buffers separately 
                    find = bufferSize;
                    findSeparately = true;
                }

                // we need to find either a single contiguous space containing 2vA unique values (which will be split up into two buffers of size vA each), 
                // or we need to find one buffer of < 2vA unique values, and a second buffer of vA unique values, 
                // OR if we couldn't find that many unique values, we need the largest possible buffer we can get 

                // in the case where it couldn't find a single buffer of at least vA unique values, 
                // all of the Merge steps must be replaced by a different merge algorithm (MergeInPlace) 

                iterator.Begin();
                Range a;
                Range b;
                while (!iterator.Finished())
                {
                    a = iterator.Next();
                    b = iterator.Next();

                    int last;
                    for (last = a.Start, count = 1; count < find; last = index, ++count)
                    {
                        index = this.FindLastForward(last, new Range(last + 1, a.End), find - count);
                        if (index == a.End)
                        {
                            break;
                        }
                    }
                    index = last;

                    if (count >= bufferSize)
                    {
                        // keep track of the range within the array where we'll need to "pull out" these values to create the internal buffer 
                        pull[pullIndex].Range = new Range(a.Start, b.End);
                        pull[pullIndex].Count = count;
                        pull[pullIndex].From = index;
                        pull[pullIndex].To = a.Start;
                        pullIndex = 1;

                        if (count == bufferSize + bufferSize)
                        {
                            // we were able to find a single contiguous section containing 2vA unique values, 
                            // so this section can be used to contain both of the internal buffers we'll need 
                            buffer1 = new Range(a.Start, a.Start + bufferSize);
                            buffer2 = new Range(a.Start + bufferSize, a.Start + count);
                            break;
                        }
                        if (find == bufferSize + bufferSize)
                        {
                            // we found a buffer that contains at least vA unique values, but did not contain the full 2vA unique values, 
                            // so we still need to find a second separate buffer of at least vA unique values 
                            buffer1 = new Range(a.Start, a.Start + count);
                            find = bufferSize;
                        }
                        else if (findSeparately)
                        {
                            // found one buffer, but now find the other one 
                            buffer1 = new Range(a.Start, a.Start + count);
                            findSeparately = false;
                        }
                        else
                        {
                            // we found a second buffer in an 'A' subarray containing vA unique values, so we're done! 
                            buffer2 = new Range(a.Start, a.Start + count);
                            break;
                        }
                    }
                    else if (pullIndex == 0 && count > buffer1.Length)
                    {
                        // keep track of the largest buffer we were able to find 
                        buffer1 = new Range(a.Start, a.Start + count);

                        pull[pullIndex].Range = new Range(a.Start, b.End);
                        pull[pullIndex].Count = count;
                        pull[pullIndex].From = index;
                        pull[pullIndex].To = a.Start;
                    }

                    // check B for the number of unique values we need to fill an internal buffer 
                    // these values will be pulled out to the end of B 
                    for (last = b.End - 1, count = 1; count < find; last = index - 1, count++)
                    {
                        index = this.FindFirstBackward(last, new Range(b.Start, last), find - count);
                        if (index == b.Start)
                        {
                            break;
                        }
                    }
                    index = last;

                    if (count >= bufferSize)
                    {
                        // keep track of the range within the array where we'll need to "pull out" these values to create the internal buffer 
                        pull[pullIndex].Range = new Range(a.Start, b.End);
                        pull[pullIndex].Count = count;
                        pull[pullIndex].From = index;
                        pull[pullIndex].To = b.End;
                        pullIndex = 1;

                        if (count == bufferSize + bufferSize)
                        {
                            // we were able to find a single contiguous section containing 2vA unique values, 
                            // so this section can be used to contain both of the internal buffers we'll need 
                            buffer1 = new Range(b.End - count, b.End - bufferSize);
                            buffer2 = new Range(b.End - bufferSize, b.End);
                            break;
                        }
                        if (find == bufferSize + bufferSize)
                        {
                            // we found a buffer that contains at least vA unique values, but did not contain the full 2vA unique values, 
                            // so we still need to find a second separate buffer of at least vA unique values 
                            buffer1 = new Range(b.End - count, b.End);
                            find = bufferSize;
                        }
                        else if (findSeparately)
                        {
                            // found one buffer, but now find the other one 
                            buffer1 = new Range(b.End - count, b.End);
                            findSeparately = false;
                        }
                        else
                        {
                            // buffer2 will be pulled out from a 'B' subarray, so if the first buffer was pulled out from the corresponding 'A' subarray, 
                            // we need to adjust the end point for that A subarray so it knows to stop redistributing its values before reaching buffer2 
                            if (pull[0].Range.Start == a.Start)
                            {
                                pull[0].Range.End -= pull[1].Count;
                            }

                            // we found a second buffer in an 'B' subarray containing vA unique values, so we're done! 
                            buffer2 = new Range(b.End - count, b.End);
                            break;
                        }
                    }
                    else if (pullIndex == 0 && count > buffer1.Length)
                    {
                        // keep track of the largest buffer we were able to find 
                        buffer1 = new Range(b.End - count, b.End);

                        pull[pullIndex].Range = new Range(a.Start, b.End);
                        pull[pullIndex].Count = count;
                        pull[pullIndex].From = index;
                        pull[pullIndex].To = b.End;
                    }
                }

                // pull out the two ranges so we can use them as internal buffers 
                for (pullIndex = 0; pullIndex < 2; pullIndex++)
                {
                    var length = pull[pullIndex].Count;

                    if (pull[pullIndex].To < pull[pullIndex].From)
                    {
                        // we're pulling the values out to the left, which means the start of an A subarray 
                        index = pull[pullIndex].From;
                        for (count = 1; count < length; count++)
                        {
                            index = this.FindFirstBackward(index - 1,
                                                           new Range(pull[pullIndex].To,
                                                                     pull[pullIndex].From - (count - 1)),
                                                           (length - count));
                            var range = new Range(index + 1, pull[pullIndex].From + 1);
                            this.Rotate(range.Length - count, range);
                            pull[pullIndex].From = index + count;
                        }
                    }
                    else if (pull[pullIndex].To > pull[pullIndex].From)
                    {
                        // we're pulling values out to the right, which means the end of a B subarray 
                        index = pull[pullIndex].From + 1;
                        for (count = 1; count < length; count++)
                        {
                            index = this.FindLastForward(index, new Range(index, pull[pullIndex].To),
                                                         length - count);
                            var range = new Range(pull[pullIndex].From, index - 1);
                            this.Rotate(count, range);
                            pull[pullIndex].From = index - 1 - count;
                        }
                    }
                }

                // adjust blockSize and buffer_size based on the values we were able to pull out 
                bufferSize = buffer1.Length;
                blockSize = iterator.Length / bufferSize + 1;

                // now that the two internal buffers have been created, it's time to merge each A+B combination at this level of the merge sort! 
                iterator.Begin();
                while (!iterator.Finished())
                {
                    a = iterator.Next();
                    b = iterator.Next();

                    // remove any parts of A or B that are being used by the internal buffers 
                    var start = a.Start;
                    if (start == pull[0].Range.Start)
                    {
                        if (pull[0].From > pull[0].To)
                        {
                            a.Start += pull[0].Count;

                            // if the internal buffer takes up the entire A or B subarray, then there's nothing to merge 
                            // this only happens for very small subarrays, like v4 = 2, 2 * (2 internal buffers) = 4, 
                            // which also only happens when cache_size is small or 0 since it'd otherwise use MergeExternal 
                            if (a.Length == 0)
                            {
                                continue;
                            }
                        }
                        else if (pull[0].From < pull[0].To)
                        {
                            b.End -= pull[0].Count;
                            if (b.Length == 0)
                            {
                                continue;
                            }
                        }
                    }
                    if (start == pull[1].Range.Start)
                    {
                        if (pull[1].From > pull[1].To)
                        {
                            a.Start += pull[1].Count;
                            if (a.Length == 0)
                            {
                                continue;
                            }
                        }
                        else if (pull[1].From < pull[1].To)
                        {
                            b.End -= pull[1].Count;
                            if (b.Length == 0)
                            {
                                continue;
                            }
                        }
                    }

                    if (Key.CompareSized(this[b.End - 1], this[a.Start], this.keyLength) < 0)
                    {
                        // the two ranges are in reverse order, so a simple rotation should fix it 
                        this.Rotate(a.Length, new Range(a.Start, b.End));
                    }
                    else if (Key.CompareSized(this[a.End], this[a.End - 1], this.keyLength) < 0)
                    {
                        // these two ranges weren't already in order, so we'll need to merge them! 

                        // break the remainder of A into blocks. firstA is the uneven-sized first A block 
                        var blockA = new Range(a.Start, a.End);
                        var firstA = new Range(a.Start, a.Start + blockA.Length % blockSize);

                        // swap the first value of each A block with the value in buffer1 
                        var indexA = buffer1.Start;
                        for (index = firstA.End; index < blockA.End; index += blockSize)
                        {
                            this.SwapEntries(this[indexA], this[index]);
                            indexA++;
                        }

                        // start rolling the A blocks through the B blocks! 
                        // whenever we leave an A block behind, we'll need to merge the previous A block with any B blocks that follow it, so track that information as well 
                        var lastA = new Range(firstA.Start, firstA.End);
                        var lastB = new Range();
                        var blockB = new Range(b.Start, b.Start + Math.Min(blockSize, b.Length));
                        blockA.Start += firstA.Length;
                        indexA = buffer1.Start;

                        // if the first unevenly sized A block fits into the cache, copy it there for when we go to Merge it 
                        // otherwise, if the second buffer is available, block swap the contents into that 
                        if (buffer2.Length > 0)
                        {
                            this.BlockSwap(lastA.Start, buffer2.Start, lastA.Length);
                        }

                        if (blockA.Length > 0)
                        {
                            while (true)
                            {
                                // if there's a previous B block and the first value of the minimum A block is <= the last value of the previous B block, 
                                // then drop that minimum A block behind. or if there are no B blocks left then keep dropping the remaining A blocks. 
                                if ((lastB.Length > 0 &&
                                     Key.CompareSized(this[lastB.End - 1], this[indexA], this.keyLength) >= 0)
                                    || blockB.Length == 0)
                                {
                                    // figure out where to split the previous B block, and rotate it at the split 
                                    var B_split = this.BinaryFirst(indexA, lastB);
                                    var B_remaining = lastB.End - B_split;

                                    // swap the minimum A block to the beginning of the rolling A blocks 
                                    var minA = blockA.Start;
                                    for (var findA = minA + blockSize; findA < blockA.End; findA += blockSize)
                                    {
                                        if (Key.CompareSized(this[findA], this[minA], this.keyLength) < 0)
                                        {
                                            minA = findA;
                                        }
                                    }

                                    this.BlockSwap(blockA.Start, minA, blockSize);

                                    this.SwapEntries(this[blockA.Start], this[indexA]);
                                    indexA++;

                                    // locally merge the previous A block with the B values that follow it 
                                    // if lastA fits into the external cache we'll use that (with MergeExternal), 
                                    // or if the second internal buffer exists we'll use that (with MergeInternal), 
                                    // or failing that we'll use a strictly in-place merge algorithm (MergeInPlace) 
                                    if (buffer2.Length > 0)
                                    {
                                        this.MergeInternal(lastA, new Range(lastA.End, B_split), buffer2);
                                    }
                                    else
                                    {
                                        this.MergeInPlace(lastA, new Range(lastA.End, B_split));
                                    }

                                    if (buffer2.Length > 0)
                                    {
                                        // copy the previous A block into the cache or buffer2, since that's where we need it to be when we go to merge it anyway 
                                        this.BlockSwap(blockA.Start, buffer2.Start, blockSize);

                                        // this is equivalent to rotating, but faster 
                                        // the area normally taken up by the A block is either the contents of buffer2, or data we don't need anymore since we memcopied it 
                                        // either way, we don't need to retain the order of those items, so instead of rotating we can just block swap B to where it belongs 
                                        this.BlockSwap(B_split, blockA.Start + blockSize - B_remaining, B_remaining);
                                    }
                                    else
                                    {
                                        // we are unable to use the 'buffer2' trick to speed up the rotation operation since buffer2 doesn't exist, so perform a normal rotation 
                                        this.Rotate(blockA.Start - B_split,
                                                    new Range(B_split, blockA.Start + blockSize));
                                    }

                                    // update the range for the remaining A blocks, and the range remaining from the B block after it was split 
                                    lastA = new Range(blockA.Start - B_remaining,
                                                      blockA.Start - B_remaining + blockSize);
                                    lastB = new Range(lastA.End, lastA.End + B_remaining);

                                    // if there are no more A blocks remaining, this step is finished! 
                                    blockA.Start += blockSize;
                                    if (blockA.Length == 0)
                                    {
                                        break;
                                    }
                                }
                                else if (blockB.Length < blockSize)
                                {
                                    // move the last B block, which is unevenly sized, to before the remaining A blocks, by using a rotation 
                                    // the cache is disabled here since it might contain the contents of the previous A block 
                                    this.Rotate(-blockB.Length, new Range(blockA.Start, blockB.End));

                                    lastB = new Range(blockA.Start, blockA.Start + blockB.Length);
                                    blockA.Start += blockB.Length;
                                    blockA.End += blockB.Length;
                                    blockB.End = blockB.Start;
                                }
                                else
                                {
                                    // roll the leftmost A block to the end by swapping it with the next B block 
                                    this.BlockSwap(blockA.Start, blockB.Start, blockSize);
                                    lastB = new Range(blockA.Start, blockA.Start + blockSize);

                                    blockA.Start += blockSize;
                                    blockA.End += blockSize;
                                    blockB.Start += blockSize;
                                    blockB.End += blockSize;

                                    if (blockB.End > b.End)
                                    {
                                        blockB.End = b.End;
                                    }
                                }
                            }
                        }

                        // merge the last A block with the remaining B values 
                        if (buffer2.Length > 0)
                        {
                            this.MergeInternal(lastA, new Range(lastA.End, b.End), buffer2);
                        }
                        else
                        {
                            this.MergeInPlace(lastA, new Range(lastA.End, b.End));
                        }
                    }
                }

                // when we're finished with this merge step we should have the one or two internal buffers left over, where the second buffer is all jumbled up 
                // insertion sort the second buffer, then redistribute the buffers back into the array using the opposite process used for creating the buffer 

                // while an unstable sort like quick sort could be applied here, in benchmarks it was consistently slightly slower than a simple insertion sort, 
                // even for tens of millions of items. this may be because insertion sort is quite fast when the data is already somewhat sorted, like it is here 
                this.InsertionSort(buffer2);

                for (pullIndex = 0; pullIndex < 2; pullIndex++)
                {
                    var unique = pull[pullIndex].Count * 2;
                    if (pull[pullIndex].From > pull[pullIndex].To)
                    {
                        // the values were pulled out to the left, so redistribute them back to the right 
                        var buffer = new Range(pull[pullIndex].Range.Start,
                                               pull[pullIndex].Range.Start + pull[pullIndex].Count);
                        while (buffer.Length > 0)
                        {
                            index = this.FindFirstForward(buffer.Start,
                                                          new Range(buffer.End, pull[pullIndex].Range.End), unique);
                            var amount = index - buffer.End;
                            this.Rotate(buffer.Length, new Range(buffer.Start, index));
                            buffer.Start += (amount + 1);
                            buffer.End += amount;
                            unique -= 2;
                        }
                    }
                    else if (pull[pullIndex].From < pull[pullIndex].To)
                    {
                        // the values were pulled out to the right, so redistribute them back to the left 
                        var buffer = new Range(pull[pullIndex].Range.End - pull[pullIndex].Count,
                                               pull[pullIndex].Range.End);
                        while (buffer.Length > 0)
                        {
                            index = this.FindLastBackward(buffer.End - 1,
                                                          new Range(pull[pullIndex].Range.Start, buffer.Start),
                                                          unique);
                            var amount = buffer.Start - index;
                            this.Rotate(amount, new Range(index, buffer.End));
                            buffer.Start -= amount;
                            buffer.End -= (amount + 1);
                            unique -= 2;
                        }
                    }
                }

                // double the size of each A and B subarray that will be merged in the next level 
                if (!iterator.NextLevel())
                {
                    break;
                }
            }
        }

        private sealed class Iterator
        {
            private readonly int denominator;
            private readonly int size;
            private int Decimal;
            private int decimalStep;
            private int numerator;
            private int numeratorStep;

            public Iterator(int size, int minLevel)
            {
                this.size = size;
                var powerOfTwo = FloorPowerOfTwo(this.size);
                this.denominator = powerOfTwo / minLevel;
                this.numeratorStep = this.size % this.denominator;
                this.decimalStep = this.size / this.denominator;

                this.Begin();
            }

            public int Length
            {
                get { return this.decimalStep; }
            }

            private static int FloorPowerOfTwo(int value)
            {
                value |= (value >> 1);
                value |= (value >> 2);
                value |= (value >> 4);
                value |= (value >> 8);
                value |= (value >> 16);

                return value - (value >> 1);
            }

            public void Begin()
            {
                this.numerator = this.Decimal = 0;
            }

            public Range Next()
            {
                var start = this.Decimal;
                this.Decimal += this.decimalStep;
                this.numerator += this.numeratorStep;

                if (this.numerator >= this.denominator)
                {
                    this.numerator -= this.denominator;
                    this.Decimal++;
                }

                return new Range(start, this.Decimal);
            }

            public bool NextLevel()
            {
                this.decimalStep += this.decimalStep;
                this.numeratorStep += this.numeratorStep;
                if (this.numeratorStep >= this.denominator)
                {
                    this.numeratorStep -= this.denominator;
                    this.decimalStep++;
                }

                return (this.decimalStep < this.size);
            }

            public bool Finished()
            {
                return this.Decimal >= this.size;
            }
        }

        private struct Pull
        {
            public int Count;
            public int From;
            public Range Range;
            public int To;
        }

        private struct Range
        {
            public int End;
            public int Start;

            public Range(int start, int end)
            {
                this.Start = start;
                this.End = end;
            }

            public int Length
            {
                get { return this.End - this.Start; }
            }
        }

        #region sort helpers
        private void NetSwap(int[] order, Range range, int x, int y)
        {
            var entryX = this[range.Start + x];
            var entryY = this[range.Start + y];
            var cmp = Key.CompareSized(entryX, entryY, this.keyLength);
            if (cmp > 0 || (order[x] > order[y] && cmp == 0))
            {
                this.SwapEntries(entryX, entryY);
                var orderSwap = order[x];
                order[x] = order[y];
                order[y] = orderSwap;
            }
        }

        // rotate the values in an array ([0 1 2 3] becomes [1 2 3 0] if we rotate by 1) 
        // this assumes that 0 <= amount <= range.Length 
        private void Rotate(int amount, Range range)
        {
            if (range.Length == 0)
            {
                return;
            }

            var split = (amount >= 0 ? range.Start + amount : range.End + amount);

            var range1 = new Range(range.Start, split);
            var range2 = new Range(split, range.End);

            this.Reverse(range1);
            this.Reverse(range2);
            this.Reverse(range);
        }

        // reverse a range of values within the array 
        private void Reverse(Range range)
        {
            for (var index = range.Length / 2 - 1; index >= 0; index--)
            {
                this.SwapEntries(this[range.Start + index], this[range.End - index - 1]);
            }
        }

        // swap a series of values in the array 
        private void BlockSwap(int start1, int start2, int blockSize)
        {
            for (var index = 0; index < blockSize; index++)
            {
                this.SwapEntries(this[start1 + index], this[start2 + index]);
            }
        }

        // merge operation using an internal buffer 
        private void MergeInternal(Range a, Range b, Range bufRange)
        {
            // whenever we find a value to add to the final array, swap it with the value that's already in that spot 
            // when this algorithm is finished, 'buffer' will contain its original contents, but in a different order 
            int aCount = 0, bCount = 0, insert = 0;

            if (b.Length > 0 && a.Length > 0)
            {
                while (true)
                {
                    if (Key.CompareSized(this[b.Start + bCount], this[bufRange.Start + aCount], this.keyLength) >= 0)
                    {
                        this.SwapEntries(this[a.Start + insert], this[bufRange.Start + aCount]);
                        aCount++;
                        insert++;
                        if (aCount >= a.Length)
                        {
                            break;
                        }
                    }
                    else
                    {
                        this.SwapEntries(this[a.Start + insert], this[b.Start + bCount]);
                        bCount++;
                        insert++;
                        if (bCount >= b.Length)
                        {
                            break;
                        }
                    }
                }
            }

            // swap the remainder of A into the final array 
            this.BlockSwap(bufRange.Start + aCount, a.Start + insert, a.Length - aCount);
        }

        // merge operation without a buffer 
        private void MergeInPlace(Range a, Range b)
        {
            if (a.Length == 0 || b.Length == 0)
            {
                return;
            }

            /* 
          this just repeatedly binary searches into B and rotates A into position. 
          the paper suggests using the 'rotation-based Hwang and Lin algorithm' here, 
          but I decided to stick with this because it had better situational performance 
           
          (Hwang and Lin is designed for merging subarrays of very different sizes, 
          but WikiSort almost always uses subarrays that are roughly the same size) 
           
          normally this is incredibly suboptimal, but this function is only called 
          when none of the A or B blocks in any subarray contained 2vA unique values, 
          which places a hard limit on the number of times this will ACTUALLY need 
          to binary search and rotate. 
           
          according to my analysis the worst case is vA rotations performed on vA items 
          once the constant factors are removed, which ends up being O(n) 
           
          again, this is NOT a general-purpose solution – it only works well in this case! 
          kind of like how the O(n^2) insertion sort is used in some places 
          */

            a = new Range(a.Start, a.End);
            b = new Range(b.Start, b.End);

            while (true)
            {
                // find the first place in B where the first item in A needs to be inserted 
                var mid = this.BinaryFirst(a.Start, b);

                // rotate A into place 
                var amount = mid - a.End;
                this.Rotate(-amount, new Range(a.Start, mid));
                if (b.End == mid)
                {
                    break;
                }

                // calculate the new A and B ranges 
                b.Start = mid;
                a = new Range(a.Start + amount, b.Start);
                a.Start = this.BinaryLast(a.Start, a);
                if (a.Length == 0)
                {
                    break;
                }
            }
        }

        // n^2 sorting algorithm used to sort tiny chunks of the full array 
        private void InsertionSort(Range range)
        {
            for (int j, i = range.Start + 1; i < range.End; i++)
            {
                var temp = stackalloc byte[this.entrySizeBytes];
                this.CopyEntry(this[i], temp);

                for (j = i;
                     j > range.Start && Key.CompareSized(temp, this[j - 1], this.keyLength) < 0;
                     --j)
                {
                    this.CopyEntry(this[j - 1], this[j]);
                }
                this.CopyEntry(temp, this[j]);
            }
        }

        // combine a linear search with a binary search to reduce the number of comparisons in situations 
        // where have some idea as to how many unique values there are and where the next value might be 
        private int FindFirstForward(int entryOffset, Range range, int unique)
        {
            if (range.Length == 0)
            {
                return range.Start;
            }
            int index, skip = Math.Max(range.Length / unique, 1);

            for (index = range.Start + skip;
                 Key.CompareSized(this[index - 1], this[entryOffset], this.keyLength) < 0;
                 index += skip)
            {
                if (index >= range.End - skip)
                {
                    return this.BinaryFirst(entryOffset, new Range(index, range.End));
                }
            }

            return this.BinaryFirst(entryOffset, new Range(index - skip, index));
        }

        private int FindFirstBackward(int entryOffset, Range range, int unique)
        {
            if (range.Length == 0)
            {
                return range.Start;
            }

            var skip = Math.Max(range.Length / unique, 1);

            int idx;
            for (idx = range.End - skip;
                 idx > range.Start && Key.CompareSized(this[idx - 1], this[entryOffset], this.keyLength) >= 0;
                 idx -= skip)
            {
                if (idx < range.Start + skip)
                {
                    return this.BinaryFirst(entryOffset, new Range(range.Start, idx));
                }
            }

            return this.BinaryFirst(entryOffset, new Range(idx, idx + skip));
        }

        private int FindLastForward(int entryOffset, Range range, int unique)
        {
            if (range.Length == 0)
            {
                return range.Start;
            }

            var skip = Math.Max(range.Length / unique, 1);

            int idx;
            for (idx = range.Start + skip;
                 Key.CompareSized(this[entryOffset], this[idx - 1], this.keyLength) >= 0;
                 idx += skip)
            {
                if (idx >= range.End - skip)
                {
                    return this.BinaryLast(entryOffset, new Range(idx, range.End));
                }
            }

            return this.BinaryLast(entryOffset, new Range(idx - skip, idx));
        }

        private int FindLastBackward(int entryOffset, Range range, int unique)
        {
            if (range.Length == 0)
            {
                return range.Start;
            }
            int index, skip = Math.Max(range.Length / unique, 1);

            for (index = range.End - skip;
                 index > range.Start &&
                 Key.CompareSized(this[entryOffset], this[index - 1], this.keyLength) < 0;
                 index -= skip)
            {
                if (index < range.Start + skip)
                {
                    return this.BinaryLast(entryOffset, new Range(range.Start, index));
                }
            }

            return this.BinaryLast(entryOffset, new Range(index, index + skip));
        }

        private int BinaryFirst(int entryOffset, Range range)
        {
            var start = range.Start;
            var end = range.End - 1;
            while (start < end)
            {
                var mid = start + (end - start) / 2;
                if (Key.CompareSized(this[mid], this[entryOffset], this.keyLength) < 0)
                {
                    start = mid + 1;
                }
                else
                {
                    end = mid;
                }
            }

            if (start == range.End - 1 &&
                Key.CompareSized(this[start], this[entryOffset], this.keyLength) < 0)
            {
                ++start;
            }

            return start;
        }

        private int BinaryLast(int entryOffset, Range range)
        {
            var start = range.Start;
            var end = range.End - 1;
            while (start < end)
            {
                var mid = start + (end - start) / 2;
                if (Key.CompareSized(this[entryOffset], this[mid], this.keyLength) >= 0)
                {
                    start = mid + 1;
                }
                else
                {
                    end = mid;
                }
            }

            if (start == range.End - 1 && Key.CompareSized(this[entryOffset], this[start], this.keyLength) >= 0)
            {
                ++start;
            }

            return start;
        }

        // For both these methods we are guaranteed data in increments of two bytes so use that to copy.
        private void CopyEntry(byte* source, byte* dest)
        {
            for (var i = 0; i < this.entrySizeBytes / 2; ++i)
            {
                *((ushort*)dest + i) = *((ushort*)source + i);
            }
        }

        private void SwapEntries(byte* first, byte* second)
        {
            for (var i = 0; i < this.entrySizeBytes / 2; ++i)
            {
                var swap = *((short*)first + i);
                *((short*)first + i) = *((short*)second + i);
                *((short*)second + i) = swap;
            }
        }
        #endregion

        #endregion

        #region enumeration
        public IEnumerator<KeyValuePair<Key, long>> GetEnumerator()
        {
            return new Enumerator(this, null);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public IEnumerable<KeyValuePair<Key, long>> GetMatchingPairs(Key filter)
        {
            using (var enumerator = new Enumerator(this, filter))
            {
                while (enumerator.MoveNext())
                {
                    yield return enumerator.Current;
                }
            }
        }

        public IEnumerable<long> GetMatchingValues(Key filter)
        {
            using (var enumerator = new Enumerator(this, filter))
            {
                while (enumerator.MoveNext())
                {
                    yield return enumerator.Current.Value;
                }
            }
        }

        private sealed class Enumerator : IEnumerator<KeyValuePair<Key, long>>
        {
            private readonly Key filterKey;

            private readonly Key key;
            private int index;
            private BufferedKeyedData<TValue> source;

            public Enumerator(BufferedKeyedData<TValue> source, Key filterKey)
            {
                this.source = source;
                this.filterKey = filterKey;

                this.key = new Key(new uint[this.source.DimensionSet.dimensions.Length]);
                if (this.source.converted)
                {
                    for (var i = 0; i < this.key.Values.Length; ++i)
                    {
                        this.key.Values[i] = Key.WildcardDimensionValue;
                    }
                }
            }

            public bool MoveNext()
            {
                while (this.index < this.source.Count)
                {
                    var offset = this.index * this.source.entrySizeBytes;
                    for (var i = 0; i < this.source.keyLength; ++i)
                    {
                        var dimensionValue = *(uint*)(this.source.DataBuffer + offset);
                        // Note: source may be marked 'converted' without a map being created if it was converted to
                        // an identical dimension set.
                        if (this.source.conversionMap != null)
                        {
                            if (this.source.conversionMap[i] != UnmappedDimension)
                            {
                                this.key[this.source.conversionMap[i]] = dimensionValue;
                            }
                        }
                        else
                        {
                            this.key[i] = dimensionValue;
                        }

                        offset += sizeof(uint);
                    }

                    ++this.index;
                    if (this.filterKey == null || this.filterKey.Matches(this.key))
                    {
                        this.Current = new KeyValuePair<Key, long>(this.key,
                                                                     ReadFixedLengthValueFromBuffer(this.source.DataBuffer + offset));
                        return true;
                    }
                }

                return false;
            }

            public void Reset()
            {
                this.index = 0;
            }

            public void Dispose()
            {
                this.source = null;
            }

            public KeyValuePair<Key, long> Current { get; private set; }

            object IEnumerator.Current
            {
                get { return this.Current; }
            }
        }
        #endregion
    }
}
