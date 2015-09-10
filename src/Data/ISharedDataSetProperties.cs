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

    using Microsoft.IO;

    /// <summary>
    /// Properties provided by data managers.
    /// </summary>
    internal interface ISharedDataSetProperties
    {
        /// <summary>
        /// Path for storing data for later use. If not set data will not be written once sealed.
        /// </summary>
        string StoragePath { get; }

        /// <summary>
        /// Value to use as the source name for locally-modified data sets.
        /// </summary>
        string LocalSourceName { get; }

        /// <summary>
        /// Timespan after which older data buckets will be 'sealed' within individual counters. This span is relative
        /// to the newest time provided when adding new data to a data set. The default is one minute, meaning that
        /// any buckets with a bucket end time of a minute or more before the latest seen timestamp will be sealed
        /// and considered no longer writable. Spans of zero or less indicate no sealing will ever be done.
        /// </summary>
        TimeSpan SealTime { get; }

        /// <summary>
        /// Timespan after which older data buckets in the persistent storage will be deleted. This span is relative
        /// to the newest time provided when adding new data to a data set. The default is 28 days, meaning that
        /// any buckets older than 28 days before the latest seen timestamp will be discarded.
        /// </summary>
        TimeSpan MaximumDataAge { get; }

        /// <summary>
        /// Configuration for data compaction.
        /// </summary>
        DataCompactionConfiguration CompactionConfiguration { get; }

        /// <summary>
        /// RecyclableMemoryStream manager to use when serializing and deserializing data,
        /// </summary>
        RecyclableMemoryStreamManager MemoryStreamManager { get; }

        /// <summary>
        /// True if any long-running tasks need to be terminated in order to shut down.
        /// </summary>
        bool ShuttingDown { get; }
    }
}
