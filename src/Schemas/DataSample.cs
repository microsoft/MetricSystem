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
    using System.Collections.Generic;

    using Bond;

    /// <summary>
    /// Describes the type of content in the response.
    /// </summary>
    public enum DataSampleType
    {
        None,
        HitCount,
        Histogram,
        Percentile,
        Average,
        Maximum,
        Minimum,
    };

    [Schema]
    public class DataSample
    {
        public DataSample()
        {
            this.Name = string.Empty;
            this.Dimensions = new Dictionary<string, string>();
            this.SampleType = DataSampleType.None;
            this.Histogram = new Dictionary<long, uint>();
        }

        /// <summary>
        /// Name identifying the data.
        /// </summary>
        [Id(1), Required]
        public string Name { get; set; }

        /// <summary>
        /// Mapping of dimension name to dimension value for this data
        /// </summary>
        [Id(2), Required, Type(typeof(Dictionary<string, string>))]
        public IDictionary<string, string> Dimensions { get; set; }

        /// <summary>
        /// Timestamp in millisecond granularity marking the beginning of the sample. Time 0 is the Unix epoch
        /// (1/1/1970 00:00:00 UTC)
        /// </summary>
        [Id(3), Required]
        public long StartTime { get; set; }

        /// <summary>
        /// Timestamp in millisecond granularity marking the end of the sample. Time 0 is the Unix epoch
        /// (1/1/1970 00:00:00 UTC)
        /// </summary>
        [Id(4), Required]
        public long EndTime { get; set; }

        /// <summary>
        /// The type of sample being provided.
        /// </summary>
        [Id(5), Required]
        public DataSampleType SampleType { get; set; }

        /// <summary>
        /// [Used if sample is of type HitCount] Number of times the event has occurred
        /// </summary>
        [Id(10)]
        public long HitCount { get; set; }

        /// <summary>
        /// [Used if sample is of type Histogram] Frequency table for histogram counters
        /// </summary>
        [Id(11), Type(typeof(Dictionary<long, uint>))]
        public IDictionary<long, uint> Histogram { get; set; }

        /// <summary>
        /// Total number of samples in the aggregate sample (Histogram, Avg, Percentile, Min, Max)
        /// </summary>
        [Id(12)]
        public ulong SampleCount { get; set; }

        /// <summary>
        /// [Used if sample is of type Average] 
        /// </summary>
        [Id(13)]
        public double Average { get; set; }

        /// <summary>
        /// [Used if sample is of type Percentile] The percentile being reported on [0-100]
        /// </summary>
        [Id(14)]
        public double Percentile { get; set; }

        /// <summary>
        /// [Used if sample is of type Percentile] The value of the data at the reported percentile
        /// </summary>
        [Id(15)]
        public long PercentileValue { get; set; }

        /// <summary>
        /// [Used if sample is of type Minimum] The minimum value seen
        /// </summary>
        [Id(16)]
        public long MinValue { get; set; }

        /// <summary>
        /// [Used if sample is of type Maximum] The maximum value seen
        /// </summary>
        [Id(17)]
        public long MaxValue { get; set; }

        /// <summary>
        /// If this response is aggregated from multiple machines, the total number of machines which returned data
        /// </summary>
        [Id(100)]
        public uint MachineCount { get; set; }
    }
}
