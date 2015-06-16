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

    /// <summary>
    /// A set of extension methods to make it convenient to write to counters which may not be initialized (null).
    /// These are provided for convenient use of code which may wish to write to counters which have not been configured
    /// at the time the code is released.
    /// </summary>
    public static class CounterExtensionMethods
    {
        /// <summary>
        /// Safely increment a HitCounter by one using the current time.
        /// If the counter is null no operation will be performed.
        /// </summary>
        /// <param name="counter">Counter to increment.</param>
        /// <param name="dims">Dimensions to use for incrementing.</param>
        public static void SafeIncrement(this HitCounter counter, DimensionSpecification dims)
        {
            if (counter != null)
            {
                counter.Increment(1, dims, DateTime.Now);
            }
        }

        /// <summary>
        /// Safely increment a HitCounter by one using the given timestamp.
        /// If the counter is null no operation will be performed.
        /// </summary>
        /// <param name="counter">Counter to increment.</param>
        /// <param name="dims">Dimensions to use for incrementing.</param>
        /// <param name="timestamp">Timestamp for written value.</param>
        public static void SafeIncrement(this HitCounter counter, DimensionSpecification dims, DateTime timestamp)
        {
            if (counter != null)
            {
                counter.Increment(1, dims, timestamp);
            }
        }

        /// <summary>
        /// Safely increment a HitCounter by the given amount using the current time.
        /// If the counter is null no operation will be performed.
        /// </summary>
        /// <param name="counter">Counter to increment.</param>
        /// <param name="amount">Amount to increment by.</param>
        /// <param name="dims">Dimensions to use for incrementing.</param>
        public static void SafeIncrement(this HitCounter counter, long amount, DimensionSpecification dims)
        {
            if (counter != null)
            {
                counter.Increment(amount, dims, DateTime.Now);
            }
        }

        /// <summary>
        /// Safely increment a HitCounter by a given amount.
        /// If the counter is null no operation will be performed.
        /// </summary>
        /// <param name="counter">Counter to increment.</param>
        /// <param name="amount">Amount to increment by.</param>
        /// <param name="dims">Dimensions to use for incrementing.</param>
        /// <param name="timestamp">Timestamp for written value.</param>
        public static void SafeIncrement(this HitCounter counter, long amount, DimensionSpecification dims,
                                         DateTime timestamp)
        {
            if (counter != null)
            {
                counter.Increment(amount, dims, timestamp);
            }
        }

        /// <summary>
        /// Safely write a value to a HistogramCounter using the the current time.
        /// If the counter is null no operation will be performed.
        /// </summary>
        /// <param name="counter"></param>
        /// <param name="value"></param>
        /// <param name="dims"></param>
        public static void SafeAddValue(this HistogramCounter counter, long value, DimensionSpecification dims)
        {
            if (counter != null)
            {
                counter.AddValue(value, dims, DateTime.Now);
            }
        }

        /// <summary>
        /// Safely write a value to a HistogramCounter using the provided timestamp.
        /// If the counter is null no operation will be performed.
        /// </summary>
        /// <param name="counter"></param>
        /// <param name="value"></param>
        /// <param name="dims"></param>
        /// <param name="timestamp"></param>
        public static void SafeAddValue(this HistogramCounter counter, long value, DimensionSpecification dims,
                                        DateTime timestamp)
        {
            if (counter != null)
            {
                counter.AddValue(value, dims, timestamp);
            }
        }
    }
}
