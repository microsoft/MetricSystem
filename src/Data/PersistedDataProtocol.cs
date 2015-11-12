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

    using Microsoft.IO;

    internal static class PersistedDataProtocol
    {
        internal const ushort ProtocolVersion = 11;
        internal const ushort PreviousProtocolVersion = ProtocolVersion - 1;

        // The top byte of the 8 byte length of buffers is used to store data related to buffer compression. This leaves
        // a theoretical max buffer size of 2^56-1 bytes, or 64PB. Shouldn't be a big deal.
        private const int CompressionDataShiftWidth = 56;
        private const long CompressionTypeMask = (long)0x7f << CompressionDataShiftWidth;
        private const long CompressionFlag = (long)0x80 << CompressionDataShiftWidth;
        public const long MaxBufferSize = ((long)1 << CompressionDataShiftWidth) - 1;

        public enum CompressionType : long
        {
            GZip = 0,
            LZ4 = 1,
            None = 0x7f,
        }

        /// <summary>
        /// Deserialize a length value and detect compression settings.
        /// </summary>
        /// <param name="originalValue">Original serialized value.</param>
        /// <param name="isCompressed">Set to true if the buffer is compressed.</param>
        /// <param name="compressionType">Set to the type of compression used for the buffer. Invalid values will result in a <see cref="PersistedDataException"/> being thrown.</param>
        /// <returns>The actual length of the buffer.</returns>
        public static long DeserializeBufferLengthValue(long originalValue, out bool isCompressed,
                                                        out CompressionType compressionType)
        {
            if ((originalValue & CompressionFlag) != 0)
            {
                isCompressed = true;
                compressionType = (CompressionType)((originalValue & CompressionTypeMask) >> CompressionDataShiftWidth);
                CheckCompressionType(compressionType);
            }
            else
            {
                isCompressed = false;
                compressionType = CompressionType.None;
            }

            return originalValue & MaxBufferSize;
        }

        /// <summary>
        /// Prepares a buffer length value for serialization with the appropriate compression flags applied.
        /// </summary>
        /// <param name="bufferLength">Actual length of the buffer.</param>
        /// <param name="isCompressed">Whether the buffer is compressed.</param>
        /// <param name="compressionType">The type of compression used for the buffer. Invalid values will result in a <see cref="PersistedDataException"/> being thrown.</param>
        /// <returns>The value to serialize.</returns>
        public static long SerializeBufferLengthValue(long bufferLength, bool isCompressed,
                                                      CompressionType compressionType)
        {
            if (bufferLength > MaxBufferSize)
            {
                throw new PersistedDataException("Buffer length is larger than maximum allowed buffer size.");
            }

            if (isCompressed)
            {
                CheckCompressionType(compressionType);
                bufferLength |= CompressionFlag;
                bufferLength |= ((long)compressionType << CompressionDataShiftWidth);
            }

            return bufferLength;
        }

        private static void CheckCompressionType(CompressionType compressionType)
        {
            switch (compressionType)
            {
            case CompressionType.GZip:
                break;
            // LZ4 support coming in separate revision.
            default:
                throw new PersistedDataException("Invalid buffer compression type " + compressionType);
            }
        }

        internal static Type GetTypeFromPersistedTypeCode(PersistedDataType typeCode)
        {
            switch (typeCode)
            {
            case PersistedDataType.HitCount:
                return typeof(InternalHitCount);

            case PersistedDataType.VariableEncodedHistogram:
                return typeof(InternalHistogram);

            default:
                throw new InvalidDataException("Unknown typecode");
            }
        }

        internal static PersistedDataType GetPersistedTypeCodeFromType(Type sourceType)
        {
            if (sourceType == typeof(InternalHitCount))
            {
                return PersistedDataType.HitCount;
            }
            if (sourceType == typeof(InternalHistogram))
            {
                // we never write out the old format
                return PersistedDataType.VariableEncodedHistogram;
            }

            throw new ArgumentException("Unknown data type", "sourceType");
        }

        internal static PersistedDataType GetPersistedDataTypeFromDataSampleType(DataSampleType dataType)
        {
            switch (dataType)
            {
            case DataSampleType.HitCount:
                return PersistedDataType.HitCount;

            case DataSampleType.Histogram:
                return PersistedDataType.VariableEncodedHistogram;

            default:
                throw new ArgumentException("Invalid data type", "dataType");
            }
        }

        /// <summary>
        /// Create an aggregation client for a given data type.
        /// </summary>
        /// <param name="dataType">Type of sample to aggregate.</param>
        /// <param name="name">Name of the data to aggregate.</param>
        /// <param name="sources">List of source systems to aggregate from.</param>
        /// <param name="startTime">Start of timespan to aggregate data for.</param>
        /// <param name="endTime">End of timespan to aggregate data for.</param>
        /// <param name="streamManager">RecyclableMemoryStreamManager to use when aggregating data.</param>
        /// <returns>Suitable aggregation client.</returns>
        public static IPersistedDataAggregator CreateAggregatorForSampleType(MetricSystem.PersistedDataType dataType, string name,
                                                                             IEnumerable<string> sources,
                                                                             DateTime startTime, DateTime endTime,
                                                                             RecyclableMemoryStreamManager streamManager)
        {
            return CreateAggregatorForSampleType(dataType, name, null, sources, startTime, endTime, streamManager);
        }

        /// <summary>
        /// Create an aggregation client for a given data type.
        /// </summary>
        /// <param name="dataType">Type of sample to aggregate.</param>
        /// <param name="name">Name of the data to aggregate.</param>
        /// <param name="dimensionSet">Existing dimension set to use / populate.</param>
        /// <param name="sources">List of source systems to aggregate from.</param>
        /// <param name="startTime">Start of timespan to aggregate data for.</param>
        /// <param name="endTime">End of timespan to aggregate data for.</param>
        /// <param name="streamManager">RecyclableMemoryStreamManager for the aggregation client.</param>
        /// <returns>Suitable aggregation client.</returns>
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "User expected to own object lifetime.")]
        public static IPersistedDataAggregator CreateAggregatorForSampleType(MetricSystem.PersistedDataType dataType, string name,
                                                                             DimensionSet dimensionSet,
                                                                             IEnumerable<string> sources,
                                                                             DateTime startTime, DateTime endTime,
                                                                             RecyclableMemoryStreamManager streamManager)
        {
            switch (dataType)
            {
            case MetricSystem.PersistedDataType.HitCount:
                return new PersistedDataAggregator<InternalHitCount>(name, dimensionSet, sources, 
                                                                            startTime, endTime, streamManager);

            case MetricSystem.PersistedDataType.VariableEncodedHistogram:
                return new PersistedDataAggregator<InternalHistogram>(name, dimensionSet, sources,
                                                                            startTime, endTime, streamManager);
                
            default:
                throw new ArgumentException("Invalid data type", "dataType");
            }
        }
    }
}
