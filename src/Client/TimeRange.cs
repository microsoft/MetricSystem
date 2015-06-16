namespace MetricSystem.Client
{
    using System;
    using MetricSystem.Utilities;

    /// <summary>
    /// Helper class to group data samples into common time buckets. Represents [Start - End] inclusive. 
    /// IComparable is implemented on start time for sorting
    /// </summary>
    internal class TimeRange : IEquatable<TimeRange>, IComparable<TimeRange>
    {
        private readonly long start;
        internal DateTime Start
        {
            get { return this.start.ToDateTime(); }
        }

        private readonly long end;
        internal DateTime End
        {
            get { return this.end.ToDateTime(); }
        }

        internal TimeSpan Elapsed
        {
            get { return TimeSpan.FromMilliseconds(this.end - this.start); }
        }

        public TimeRange(DateTime start, DateTime end)
            : this(start.ToMillisecondTimestamp(), end.ToMillisecondTimestamp())
        {
        }

        public TimeRange(long start, long end)
        {
            if (start > end)
            {
                throw new ArgumentException("TimeRange Start must be before End");
            }
            this.start = start;
            this.end = end;
        }

        public bool Equals(TimeRange other)
        {
            var range = other as TimeRange;
            if (range != null)
            {
                return this.start == range.start
                       && this.end == range.end;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Does this time range overlap with another time range? 
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool IntersectsWith(TimeRange other)
        {
            if (other == null)
            {
                throw new ArgumentNullException("other");
            }

            // for the following comments, A -> this. B -> other
            return (this.start <= other.start && this.end > other.start) ||       // A starts before B and ends during/after B
                   (other.start <= this.start && other.end > this.start);         // B starts before A and ends during/after A
        }

        /// <summary>
        /// Create a time range as a super set of two existing ranges
        /// </summary>
        public static TimeRange Merge(TimeRange a, TimeRange b)
        {
            if (a == null)
            {
                throw new ArgumentNullException("a");
            }

            if (b == null)
            {
                throw new ArgumentNullException("b");
            }

            return new TimeRange(
                a.start < b.start ? a.start : b.start, 
                a.end > b.end ? a.end : b.end
                );
        }

        /// <summary>
        /// IComparable implementation based on start time
        /// </summary>
        public int CompareTo(TimeRange other)
        {
            var otherRange = other as TimeRange;
            if (otherRange != null)
            {
                return this.start.CompareTo(otherRange.start);
            }

            throw new ArgumentException("Invalid comparison request"); 
        }
    }
}
