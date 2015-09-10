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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;

    using MetricSystem.Utilities;

    /// <summary>
    /// Base type for all internal (in-memory) representations of data
    /// </summary>
    internal interface IInternalData : IMergeableData, IMergeSource
    {
        /// <summary>
        /// Writes the given object to a Binary writer.
        /// </summary>
        /// <param name="writeStream">The stream to write to</param>
        /// <returns>Number of bytes written.</returns>
        uint WriteToPersistedData(MemoryStream writeStream);

        /// <summary>
        /// Add the given value to the data.
        /// </summary>
        /// <param name="value">Value to add.</param>
        void AddValue(long value);

        /// <summary>
        /// Populate (update, not overwrite) a wire-serializable sample with this data.
        /// </summary>
        /// <param name="sample">Sample to update.</param>
        void UpdateDataSample(DataSample sample);
    }

    /// <summary>
    /// Represents an in-memory "hit count" to track the number of occurences of an event.
    /// </summary>
    internal sealed class InternalHitCount : IInternalData
    {
        public InternalHitCount() { }

        public InternalHitCount(long value)
        {
            this.HitCount = value;
        }

        /// <summary>
        /// The count of occurences.
        /// </summary>
        public long HitCount { get; set; }

        public bool MultiValue
        {
            get { return false; }
        }

        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0",
            Justification = "Null reference / AV exception is acceptable for internal type.")]
        public unsafe void ReadFromPersistedData(byte* dataBuffer, uint dataSize, bool usePreviousProtocol)
        {
            this.HitCount += *(long*)dataBuffer;
        }

        public void Clear()
        {
            this.HitCount = 0;
        }

        public void MergeFrom(IMergeSource other)
        {
            if (other is SingleValueMergeSource)
            {
                this.HitCount += ((SingleValueMergeSource)other).Value;
            }
            else
            {
                this.HitCount += (other as InternalHitCount).HitCount;
            }
        }

        public uint WriteToPersistedData(MemoryStream writeStream)
        {
            if (writeStream == null)
            {
                throw new ArgumentNullException("writeStream");
            }

            // required to be fixed-length as this data is going into a BufferedKeyedData buffer which needs to 
            // know all values offsets at all times
            var buffer = new byte[sizeof(long)];
            ByteConverter.FixedLengthEncoding.Encode(this.HitCount, buffer, writeStream);
            return sizeof(long);
        }

        public void AddValue(long value)
        {
            this.HitCount += value;
        }

        public void UpdateDataSample(DataSample sample)
        {
            if (sample == null)
            {
                throw new ArgumentNullException("sample");
            }
            sample.SampleType = DataSampleType.HitCount;
            sample.HitCount += this.HitCount;
        }
    }

    /// <summary>
    /// Represents a serializable histogram of 64-bit values
    /// </summary>
    internal sealed class InternalHistogram : IInternalData
    {
        private readonly Dictionary<long, uint> samples;

        public InternalHistogram()
        {
            this.samples = new Dictionary<long, uint>();
        }

        /// <summary>
        /// Total number of samples.
        /// </summary>
        public ulong SampleCount { get; private set; }

        public Dictionary<long, uint> Data
        {
            get
            {
                return this.samples;
            }
        }

        public bool MultiValue
        {
            get { return true; }
        }

        public void Clear()
        {
            this.SampleCount = 0;
            this.samples.Clear();
        }

        public void MergeFrom(IMergeSource other)
        {
            if (other.MultiValue)
            {
                if (other is MultiValueMergeSource)
                {
                    var mergeSource = (MultiValueMergeSource)other;
                    this.SampleCount += mergeSource.BufferedData.ReadValuesInto(this.samples, mergeSource.StartOffset);
                }
                else
                {
                    var mergeSource = other as InternalHistogram;
                    mergeSource.UpdateDictionary(this.samples);
                    this.SampleCount += mergeSource.SampleCount;
                }
            }
            else
            {
                var mergeSource = (SingleValueMergeSource)other;
                this.AddValue(mergeSource.Value);
            }
        }

        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public uint WriteToPersistedData(MemoryStream writeStream)
        {
            return BufferedValueArray.WriteVariableLengthEncodedDictionary(this.samples, (uint)this.SampleCount,
                                                                           writeStream);
        }

        public void UpdateDataSample(DataSample sample)
        {
            if (sample == null)
            {
                throw new ArgumentNullException("sample");
            }

            this.UpdateDictionary(sample.Histogram);
            sample.SampleCount += this.SampleCount;
            sample.SampleType = DataSampleType.Histogram;
        }

        public void AddValue(long value)
        {
            if (!this.samples.ContainsKey(value))
            {
                this.samples[value] = 1;
            }
            else
            {
                this.samples[value]++;
            }

            ++this.SampleCount;
        }

        /// <summary>
        /// Updates a passed-in dictionary with the values stored in this histogram (does not overwrite existing data).
        /// </summary>
        /// <param name="dict">The dictionary to update.</param>
        public void UpdateDictionary(IDictionary<long, uint> dict)
        {
            foreach (var kvp in this.samples)
            {
                if (!dict.ContainsKey(kvp.Key))
                {
                    dict[kvp.Key] = kvp.Value;
                }
                else
                {
                    dict[kvp.Key] += kvp.Value;
                }
            }
        }
    }
}
