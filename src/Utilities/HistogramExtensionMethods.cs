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

namespace MetricSystem.Utilities
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public static class ExtensionMethods
    {
        /// <summary>
        /// Given a histogram as a dictionary return the value at a given percentile.
        /// </summary>
        /// <param name="data">Histogram.</param>
        /// <param name="percentile">Percentile (from 0..100) to query.</param>
        /// <returns>The value at the given percentile.</returns>
        public static long GetValueAtPercentile(this IDictionary<long, uint> data, double percentile)
        {
            if (data == null)
            {
                return 0;
            }
            return data.GetValueAtPercentile(percentile,
                data.Values.Aggregate<uint, ulong>(0, (current, count) => current + count));
        }

        public static double GetAverageValue(this IDictionary<long, uint> data)
        {
            if (data == null)
            {
                return 0;
            }

            ulong sampleCount = Convert.ToUInt64(data.Sum(sample => sample.Value));
            return GetAverageValue(data, sampleCount);
        }

        /// <summary>
        /// Calculate the maximum value, given a histogram.
        /// </summary>
        /// <param name="data">The dictionary used to represent the histogram</param>
        /// <returns></returns>
        public static TValue GetMaximumValue<TValue>(this IDictionary<TValue, uint> data)
        {
            if (data == null || !data.Any())
            {
                return default(TValue);
            }
            return data.Keys.Max();
        }

        /// <summary>
        /// Calculate the minimum value, given a histogram.
        /// </summary>
        /// <param name="data">The dictionary used to represent the histogram</param>
        /// <returns></returns>
        public static TValue GetMinimumValue<TValue>(this IDictionary<TValue, uint> data)
        {
            if (data == null || !data.Any())
            {
                return default(TValue);
            }
            return data.Keys.Min();
        }

        /// <summary>
        /// Calculate the average value, given a histogram.
        /// </summary>
        /// <param name="data">The dictionary used to represent the histogram</param>
        /// <param name="sampleCount">Total number of samples expressed in the Histogram</param>
        /// <returns></returns>
        public static double GetAverageValue<TValue>(this IDictionary<TValue, uint> data, ulong sampleCount)
        {
            if (data == null || sampleCount == 0)
            {
                return 0;
            }

            double movingAverage = 0;

            // we have the total sampleCount. Scale each Key by the amount it contributes to the average (Val/SampleCount)
            foreach (var keyValuePair in data)
            {
                dynamic currentKey = keyValuePair.Key;
                movingAverage += currentKey * (double)keyValuePair.Value / (double)sampleCount;
            }

            return movingAverage;
        }

        /// <summary>
        /// Given a histogram as a dictionary return the value at a given percentile.
        /// </summary>
        /// <param name="data">Histogram.</param>
        /// <param name="percentile">Percentile (from 0..100) to query.</param>
        /// <param name="sampleCount">The total number of samples represted in the histogram.</param>
        /// <returns>The value at the given percentile.</returns>
        public static TValue GetValueAtPercentile<TValue>(this IDictionary<TValue, uint> data, double percentile,
            ulong sampleCount)
        {
            if (percentile < 0 || percentile > 100)
            {
                throw new ArgumentException("Percentile must be a value between 0 and 100 (inclusive)", "percentile");
            }

            if (data == null || !data.Any())
            {
                return default(TValue);
            }

            percentile /= 100;

            // Unpacked because the packed version below was not producing correct values (not sure why)
            //ulong sampleRank = (ulong)Math.Ceiling(sampleCount * percentile);
            double dblRank = Math.Round(sampleCount * percentile) + 0.5;
            ulong sampleRank = Math.Min(sampleCount, (ulong)dblRank);
            ulong count = 0;
            foreach (var key in (from k in data.Keys orderby k ascending select k))
            {
                count += data[key];
                if (count >= sampleRank)
                {
                    return key;
                }
            }

            throw new ArithmeticException(); // kind of like a math fail. :)
        }
    }
}
