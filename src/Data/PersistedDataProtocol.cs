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

        internal const long CompressionFlag = 1 << 63; // Bit used to indicate that subsequent data will be in compressed form.

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
