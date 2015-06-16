// ---------------------------------------------------------------------
// <copyright file="CounterWriteOperation.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
