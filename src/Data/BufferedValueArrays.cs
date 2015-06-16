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
    using System.IO;

    using MetricSystem.Utilities;

    /// <summary>
    /// Abstraction of a buffer of values which can be read into a dictionary. 
    /// </summary>
    internal interface IBufferedValueArray : IDisposable
    {
        /// <summary>
        /// Write the data to the output stream
        /// </summary>
        /// <param name="stream"></param>
        void Serialize(Stream stream);

        /// <summary>
        /// Is this offset valid for this buffer?
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        bool ValidateOffset(long offset);

        /// <summary>
        /// Deserialize all values in the array into a frequency dictionary
        /// </summary>
        uint ReadValuesInto(Dictionary<long, uint> destination, long startOffset);

        /// <summary>
        /// Can this buffer be written to persisted data format?
        /// </summary>
        bool CanWriteToPersistedData { get; }
    }

    /// <summary>
    /// Static class which can create instances of IBufferedValueArray's which read and write to the appropriate persisted data format
    /// </summary>
    internal static class BufferedValueArray
    {
        public const uint CompressedHistogramFlag = 0x80000000;

        public static IBufferedValueArray Create(PersistedDataType type, byte[] sourceBuffer, int startOffset,
                                                 int existingDataLength)
        {
            // legacy types are fixed-length encoded. New default is VLE
            switch (type)
            {
            case PersistedDataType.HitCount:
                return new FixedLengthBufferedValueArray<ulong>(sourceBuffer, startOffset, existingDataLength);

            case PersistedDataType.VariableEncodedHistogram:
                return new VariableLengthBufferedValueArray(sourceBuffer, startOffset, existingDataLength);

            default:
                throw new InvalidDataException("Unknown data type specfieid.");
            }
        }

        /// <summary>
        /// Create the default type of BufferedValueArray
        /// </summary>
        public static IBufferedValueArray Create(byte[] sourceBuffer, int startOffset, int existingDataLength)
        {
            return new VariableLengthBufferedValueArray(sourceBuffer, startOffset, existingDataLength);
        }

        public static uint WriteVariableLengthEncodedDictionary(Dictionary<long, uint> samples, uint totalSampleCount,
                                                                Stream writeStream)
        {
            // the first uint in the stream needs to be the size. Since we are variable length encoded, we don't know the exact size
            // until we are done. Thus, first skip the position marker
            var originalPosition = writeStream.Position;
            writeStream.Position += sizeof(uint);

            uint totalBytesWritten = 0;
            var compress = false;

            // we will compress (to a frequency dictionary) if the number of entries for a kvp dictionary is less than writing each sample
            // individually.
            if (samples.Count * 2 < totalSampleCount)
            {
                // we will write less total values if we store the kv-pairs than all individual samples. 
                // therefore...compress!
                compress = true;
                foreach (var kvp in samples)
                {
                    totalBytesWritten += ByteConverter.VariableLengthEncoding.Write(kvp.Key, writeStream);
                    totalBytesWritten += ByteConverter.VariableLengthEncoding.Write(kvp.Value, writeStream);
                }
            }
            else
            {
                // uncompressed, write the raw samples N times
                foreach (var kvp in samples)
                {
                    for (var iter = 0; iter < kvp.Value; iter++)
                    {
                        totalBytesWritten += ByteConverter.VariableLengthEncoding.Write(kvp.Key, writeStream);
                    }
                }
            }

            // now rewind to where we wrote the dummy marker
            writeStream.Position = originalPosition;

            // write the actual size
            var size = compress
                           ? totalBytesWritten | BufferedValueArray.CompressedHistogramFlag
                           : totalBytesWritten;
            var sizeData = BitConverter.GetBytes(size);
            writeStream.Write(sizeData, 0, sizeof(uint));

            // fast-forward to the end of where we wrote our contents
            writeStream.Position += totalBytesWritten;
            return totalBytesWritten + sizeof(uint);
        }

        /// <summary>
        /// Implementation of IBufferedValueArray which reads legacy payload (fixed length encoding). 
        /// </summary>
        internal unsafe class FixedLengthBufferedValueArray<TValue> : BufferedData<TValue>, IBufferedValueArray
        {
            public FixedLengthBufferedValueArray(byte[] sourceBuffer, int startOffset, int existingDataLength)
                : base(sourceBuffer, startOffset, existingDataLength)
            {
                if (this.InitialDataLength <= 0)
                {
                    throw new ArgumentOutOfRangeException("existingDataLength", "Value streams must contain some data.");
                }
            }

            protected override int ValidDataLength
            {
                get { return this.InitialDataLength; }
            }

            public bool CanWriteToPersistedData
            {
                // cannot write as-is. Need to transcode to new format
                get { return false; }
            }

            /// <summary>
            /// Read buffered values into the given destination dictionary, returning the total number of values read.
            /// </summary>
            /// <param name="destination">Dictionary to update.</param>
            /// <param name="startOffset">Start offset of the buffered data.</param>
            /// <returns></returns>
            public uint ReadValuesInto(Dictionary<long, uint> destination, long startOffset)
            {
                this.CheckDisposed();
                var dataSize = (*(uint*)(this.DataBuffer + startOffset));
                var buffer = this.DataBuffer + startOffset + sizeof(uint);

                uint totalValues = 0;
                if ((dataSize & BufferedValueArray.CompressedHistogramFlag) ==
                    BufferedValueArray.CompressedHistogramFlag)
                {
                    dataSize &= ~BufferedValueArray.CompressedHistogramFlag;
                    var valueSize = FixedLengthValueSize + sizeof(uint);
                    var valueCount = dataSize / valueSize;
                    for (var i = 0; i < valueCount; ++i)
                    {
                        var value = ReadFixedLengthValueFromBuffer(buffer + (valueSize * i));
                        var count = *(uint*)(buffer + (valueSize * i) + FixedLengthValueSize);
                        if (destination.ContainsKey(value))
                        {
                            destination[value] += count;
                        }
                        else
                        {
                            destination[value] = count;
                        }

                        totalValues += count;
                    }
                }
                else
                {
                    var valueCount = dataSize / FixedLengthValueSize;
                    for (var i = 0; i < valueCount; ++i)
                    {
                        // In future we can assume values are clumped together when written (as a result of storage
                        // changes) but for now we can't make that assumption. When values are clumped it MIGHT be
                        // slightly more efficient to count the number of contiguous values when doing a dictionary write.
                        // However, this may not matter much since uncompressed value arrays are not likely to have
                        // substantial value overlap in aggregate.
                        var value = ReadFixedLengthValueFromBuffer(buffer + (i * FixedLengthValueSize));
                        if (destination.ContainsKey(value))
                        {
                            destination[value]++;
                        }
                        else
                        {
                            destination[value] = 1;
                        }
                    }

                    totalValues = (uint)valueCount;
                }

                return totalValues;
            }
        }

        /// <summary>
        /// Variable-Length encoded buffered array. 
        /// </summary>
        internal unsafe class VariableLengthBufferedValueArray 
            // derive from BufferedData to reuse buffer pinning/cleanup logic
            : BufferedData<long>, IBufferedValueArray
        {
            public VariableLengthBufferedValueArray(byte[] sourceBuffer, int startOffset, int existingDataLength)
                : base(sourceBuffer, startOffset, existingDataLength)
            {
                if (this.InitialDataLength <= 0)
                {
                    throw new ArgumentOutOfRangeException("existingDataLength", "Value streams must contain some data.");
                }
            }

            protected override int ValidDataLength
            {
                get { return this.InitialDataLength; }
            }

            public bool CanWriteToPersistedData
            {
                // no need to transcode VLE buffers. Write as-is
                get { return true; }
            }

            /// <summary>
            /// Read buffered values into the given destination dictionary, returning the total number of values read.
            /// </summary>
            /// <param name="destination">Dictionary to update.</param>
            /// <param name="startOffset">Start offset of the buffered data.</param>
            /// <returns></returns>
            public uint ReadValuesInto(Dictionary<long, uint> destination, long startOffset)
            {
                this.CheckDisposed();

                var dataSize = (*(uint*)(this.DataBuffer + startOffset));
                var buffer = this.DataBuffer + startOffset + sizeof(uint);

                uint totalValues = 0;

                if ((dataSize & BufferedValueArray.CompressedHistogramFlag) == BufferedValueArray.CompressedHistogramFlag)
                {
                    dataSize &= ~BufferedValueArray.CompressedHistogramFlag;
                    long offset = 0;
                    while (offset < dataSize)
                    {
                        long value;
                        uint count;

                        offset += ByteConverter.VariableLengthEncoding.ReadInt64(buffer + offset, dataSize - offset, out value);
                        offset += ByteConverter.VariableLengthEncoding.ReadUInt32(buffer + offset, dataSize - offset, out count);

                        if (destination.ContainsKey(value))
                        {
                            destination[value] += count;
                        }
                        else
                        {
                            destination[value] = count;
                        }

                        totalValues += count;
                    }
                }
                else
                {
                    long offset = 0;
                    while (offset < dataSize)
                    {
                        long value;
                        offset += ByteConverter.VariableLengthEncoding.ReadInt64(buffer + offset, dataSize - offset, out value);

                        if (destination.ContainsKey(value))
                        {
                            destination[value]++;
                        }
                        else
                        {
                            destination[value] = 1;
                        }

                        totalValues++;
                    }
                }

                return totalValues;
            }
        }
    }
}
