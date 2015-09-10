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
