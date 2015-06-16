// ---------------------------------------------------------------------
// <copyright file="DateTimeExtensionMethods.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Utilities
{
    using System;

    public static class DateTimeExtensionMethods
    {
        private static readonly long BeginningOfTheEpochInTicks =
            new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

        /// <summary>
        /// Convert this date time to a Unix-style timestamp (number of milliseconds since the epoch)
        /// </summary>
        public static long ToMillisecondTimestamp(this DateTime dateTime)
        {
            DateTime dt = dateTime.Kind == DateTimeKind.Utc ? dateTime : dateTime.ToUniversalTime();
            return (dt.Ticks - BeginningOfTheEpochInTicks) / TimeSpan.TicksPerMillisecond;
        }

        /// <summary>
        /// Convert this DateTimeOffset to a Unix-style timestamp (milliseconds since the epoch)
        /// </summary>
        public static long ToMillisecondTimestamp(this DateTimeOffset dateTime)
        {
            var dt = dateTime.Offset == TimeSpan.Zero ? dateTime : dateTime.ToUniversalTime();
            return (dt.Ticks - BeginningOfTheEpochInTicks) / TimeSpan.TicksPerMillisecond;
        }

        /// <summary>
        /// Convert this Unix timestamp to a DateTime
        /// </summary>
        public static DateTime ToDateTime(this long timeInMsSinceEpoch)
        {
            var msInTicks = timeInMsSinceEpoch * TimeSpan.TicksPerMillisecond;
            return new DateTime(msInTicks + BeginningOfTheEpochInTicks,
                                DateTimeKind.Utc);
        }

        /// <summary>
        /// Convert this Unix timestamp to a DateTime
        /// </summary>
        public static DateTimeOffset ToDateTimeOffset(this long timeInMsSinceEpoch)
        {
            var msInTicks = timeInMsSinceEpoch * TimeSpan.TicksPerMillisecond;
            return new DateTimeOffset(msInTicks + BeginningOfTheEpochInTicks, TimeSpan.Zero);
        }
    }
}
