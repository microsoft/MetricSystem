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

namespace MetricSystem
{
    using System;
    using System.Collections.Generic;

    using Bond;

    [Schema]
    public sealed class CounterWriteOperation
    {
        /// <summary>
        /// Value to provide in order to request a "current wall time" Timestamp.
        /// </summary>
        public const long TimestampNow = long.MinValue;

        public CounterWriteOperation()
        {
            this.Count = 1;
            this.DimensionValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            this.Timestamp = TimestampNow;
        }

        /// <summary>
        /// Value to write into the counter.
        /// </summary>
        [Id(1), Required]
        public long Value { get; set; }

        /// <summary>
        /// Count of times to write the provided value (default is 1).
        /// </summary>
        [Id(2)]
        public long Count { get; set; }

        /// <summary>
        /// Key/value pairs of dimensions associated with the value. Not all dimensions must be provided.
        /// </summary>
        [Id(3), Type(typeof(Dictionary<string, string>))]
        public IDictionary<string, string> DimensionValues { get; set; }

        /// <summary>
        /// Timestamp in millisecond granularity for the value. Time 0 is the Unix epoch (1/1/1970 00:00:00 UTC)
        /// Specifying <see cref="TimestampNow"/> means "use current server time."
        /// </summary>
        [Id(4)]
        public long Timestamp { get; set; }
    }
}
