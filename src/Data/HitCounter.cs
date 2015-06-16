// ---------------------------------------------------------------------
// <copyright file="HitCountData.cs" company="Microsoft">
//       Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
