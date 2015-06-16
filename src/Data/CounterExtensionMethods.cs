// ---------------------------------------------------------------------
// <copyright file="CounterExtensionMethods.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
