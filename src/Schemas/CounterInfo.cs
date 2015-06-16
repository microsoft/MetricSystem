// ---------------------------------------------------------------------
// <copyright file="CounterInfo.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Bond;
    using Bond.Tag;

    [Schema]
    public sealed class CounterInfo : IComparable<CounterInfo>
    {
        public CounterInfo()
        {
            this.Name = string.Empty;
            this.Type = CounterType.Unknown;
            this.Dimensions = new List<string>();
            this.DimensionValues = new Dictionary<string, ISet<string>>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Name of the counter.
        /// </summary>
        [Id(1), Required]
        public string Name { get; set; }

        /// <summary>
        /// Type of the counter.
        /// </summary>
        [Id(2), Required]
        public CounterType Type { get; set; }

        /// <summary>
        /// Timestamp in millisecond granularity marking the beginning of counter data. Time 0 is the Unix epoch
        /// (1/1/1970 00:00:00 UTC)
        /// </summary>
        [Id(3), Required]
        public long StartTime { get; set; }

        /// <summary>
        /// Timestamp in millisecond granularity marking the end of counter data. Time 0 is the Unix epoch
        /// (1/1/1970 00:00:00 UTC)
        /// </summary>
        [Id(4), Required]
        public long EndTime { get; set; }

        /// <summary>
        /// Dimensions the counter provides.
        /// </summary>
        [Id(5), Required, Type(typeof(List<string>))]
        public IList<string> Dimensions { get; set; }

        /// <summary>
        /// Values for dimensions.
        /// </summary>
        [Id(6), Required, Type(typeof(nullable<Dictionary<string, HashSet<string>>>))]
        public IDictionary<string, ISet<string>> DimensionValues { get; set; }

        /// <summary>
        /// Add a new value for a dimension (not threadsafe).
        /// </summary>
        /// <param name="dimension">Name of the dimension.</param>
        /// <param name="newValues">Values to add.</param>
        public void AddDimensionValues(string dimension, IEnumerable<string> newValues)
        {
            if (this.DimensionValues == null)
            {
                this.DimensionValues = new Dictionary<string, ISet<string>>(StringComparer.OrdinalIgnoreCase);
            }

            ISet<string> currentValues;
            HashSet<string> values;
            if (!this.DimensionValues.TryGetValue(dimension, out currentValues))
            {
                values = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                this.DimensionValues[dimension] = values;
            }
            else
            {
                values = (HashSet<string>)currentValues;
            }

            foreach (var val in newValues)
            {
                values.Add(val);
            }
        }

        /// <summary>
        /// Fixes the sets in DimensionValues to be case insensitive. XXX: Workaround for poor Bond extensibility.
        /// </summary>
        public void FixDimensionValuesCaseSensitivity()
        {
            // If nothing in our current collection of values needs to be fixed we can bounce out early.
            if (!this.DimensionValues.Values.Select(value => value as HashSet<string>)
                    .Any(set => set == null || set.Comparer != StringComparer.OrdinalIgnoreCase))
            {
                return;
            }

            var newValues = new Dictionary<string, ISet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in this.DimensionValues)
            {
                newValues[kvp.Key] = FixSingleValueSet(kvp.Value);
            }

            this.DimensionValues = newValues;
        }

        private static HashSet<string> FixSingleValueSet(ISet<string> original)
        {
            var set = original as HashSet<string>;
            if (set != null && set.Comparer == StringComparer.OrdinalIgnoreCase)
            {
                return set;
            }

            set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var value in original)
            {
                set.Add(value);
            }
            return set;
        }

        public int CompareTo(CounterInfo other)
        {
            if (other == null)
            {
                return -1;
            }

            var cmp = StringComparer.OrdinalIgnoreCase.Compare(this.Name, other.Name);
            return cmp != 0 ? cmp : (this.Type == other.Type ? 0 : (this.Type > other.Type ? 1 : -1));
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as CounterInfo);
        }

        public bool Equals(CounterInfo counter)
        {
            return this.CompareTo(counter) == 0;
        }

        public override int GetHashCode()
        {
            return this.Name.ToLowerInvariant().GetHashCode() + (int)this.Type;
        }
    }
}
