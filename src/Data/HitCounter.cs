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

    public sealed class HitCounter : Counter
    {
        private readonly DataSet<InternalHitCount> hitCounter;

        internal HitCounter(DataSet<InternalHitCount> dataSet)
            : base(dataSet)
        {
            this.hitCounter = dataSet; // this helps avoid repeated casting.
        }

        public override CounterType Type
        {
            get { return CounterType.HitCount; }
        }

        /// <summary>
        /// Increment the counter by one for the provided dimensions using the current time for the data point.
        /// </summary>
        /// <param name="dims">Full set of dimension values for the counter.</param>
        public void Increment(DimensionSpecification dims)
        {
            this.Increment(dims, DateTime.Now);
        }

        /// <summary>
        /// Increment the counter by one for the provided dimensions.
        /// </summary>
        /// <param name="dims">Full set of dimension values for the counter.</param>
        /// <param name="timestamp">Timestamp to use for the data point.</param>
        public void Increment(DimensionSpecification dims, DateTime timestamp)
        {
            this.Increment(1, dims, timestamp);
        }

        /// <summary>
        /// Increment the counter for the provided dimensions using the current time for the data point.
        /// </summary>
        /// <param name="amount">Amount to increment the counter by.</param>
        /// <param name="dims">Full set of dimension values for the counter.</param>
        public void Increment(long amount, DimensionSpecification dims)
        {
            this.Increment(amount, dims, DateTime.Now);
        }

        /// <summary>
        /// Increment the counter for the provided dimensions.
        /// </summary>
        /// <param name="amount">Amount to increment the counter by.</param>
        /// <param name="dims">Full set of dimension values for the counter.</param>
        /// <param name="timestamp">Timestamp to use for the data point.</param>
        public void Increment(long amount, DimensionSpecification dims, DateTime timestamp)
        {
            this.hitCounter.AddValue(amount, dims, timestamp);
        }
    }
}
