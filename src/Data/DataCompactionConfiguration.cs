// ---------------------------------------------------------------------
// <copyright file="DataCompactionConfiguration.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    [JsonObject(MemberSerialization.OptIn), JsonConverter(typeof(Converter))]
    public sealed class DataCompactionConfiguration
    {
        public static readonly TimeSpan MinimumIndividualDuration = TimeSpan.FromSeconds(1);

        private static readonly List<DataIntervalConfiguration> DefaultConfiguration
            = new List<DataIntervalConfiguration>
              {
                  // One minute intervals for two hours
                  new DataIntervalConfiguration(TimeSpan.FromMinutes(1), TimeSpan.FromHours(2)),
                  // Five minute intervals for 46 hours. (2-48hrs)
                  new DataIntervalConfiguration(TimeSpan.FromMinutes(5), TimeSpan.FromHours(46)),
                  // Ten minute intervals for two days. (2-4 days)
                  new DataIntervalConfiguration(TimeSpan.FromMinutes(10), TimeSpan.FromDays(2)),
                  // Twenty minute intervals for 24 days (4-28 days)
                  new DataIntervalConfiguration(TimeSpan.FromMinutes(20), TimeSpan.FromDays(24)),
                  // One hour intervals for everything older.
                  new DataIntervalConfiguration(TimeSpan.FromHours(1), TimeSpan.MaxValue),
              };

        public DataCompactionConfiguration()
            : this(DefaultConfiguration) { }

        // [ravisr]: Could potentially load config from a configuration file
        // if necessary.
        public DataCompactionConfiguration(IEnumerable<DataIntervalConfiguration> config)
        {
            var configuration = new SortedSet<DataIntervalConfiguration>(config, new DataIntervalConfigurationComparer());
            ValidateConfig(configuration);
            this.Intervals = configuration;
        }

        [JsonProperty]
        public SortedSet<DataIntervalConfiguration> Intervals { get; private set; }

        /// <summary>
        /// True if any compaction work is enabled.
        /// </summary>
        public bool IsCompactionEnabled
        {
            get { return this.Intervals.Count > 1; }
        }

        public DataIntervalConfiguration Default
        {
            get { return this.Intervals.First(); }
        }

        public long DefaultBucketTicks
        {
            get { return this.Default.Interval.Ticks; }
        }

        /// <summary>
        /// Create a list of configurations tied to the earliest time a data bucket can apply to that config.
        /// </summary>
        /// <param name="currentTime">Current time to work backwards from.</param>
        /// <returns>A sorted list of timestamps mapped to bucket configuration for compaction.</returns>
        public SortedList<DateTime, DataIntervalConfiguration> GetEarliestTimestampsPerBucket(DateTime currentTime)
        {
            var list = new SortedList<DateTime, DataIntervalConfiguration>(this.Intervals.Count,
                                                                           Comparer<DateTime>.Create(
                                                                                                     (x, y) =>
                                                                                                     y.CompareTo(x)));

            var earliestTimeForCurrentBucket = currentTime;
            for (var i = 0; i < this.Intervals.Count - 1; ++i)
            {
                var config = this.Intervals.ElementAt(i);

                var nextBucket = this.Intervals.ElementAt(i + 1);
                earliestTimeForCurrentBucket -= config.Duration;
                earliestTimeForCurrentBucket -=
                    TimeSpan.FromTicks(earliestTimeForCurrentBucket.Ticks % nextBucket.Interval.Ticks);

                list.Add(earliestTimeForCurrentBucket, config);
            }
            list.Add(DateTime.MinValue, this.Intervals.Last());

            return list;
        }

        public override string ToString()
        {
            var ret = string.Empty;
            foreach (var bucket in this.Intervals)
            {
                ret += bucket + "\n";
            }

            return ret;
        }

        private static void ValidateConfig(SortedSet<DataIntervalConfiguration> config)
        {
            foreach (var setting in config)
            {
                if (setting.Interval < MinimumIndividualDuration)
                {
                    throw new ArgumentException("Individual bucket duration is smaller than minimum.", "config");
                }
            }

            // Last bucket duration should be set to infinity
            if (config.Last().Duration != TimeSpan.MaxValue)
            {
                throw new ArgumentException("Final bucket duration must be TimeSpan.MaxValue.", "config");
            }
        }

        private sealed class DataIntervalConfigurationComparer : IComparer<DataIntervalConfiguration>
        {
            public int Compare(DataIntervalConfiguration x, DataIntervalConfiguration y)
            {
                if (x == null)
                {
                    throw new ArgumentNullException("x");
                }
                if (y == null)
                {
                    throw new ArgumentNullException("y");
                }

                return x.Interval.CompareTo(y.Interval);
            }
        }

        private sealed class Converter : JsonConverter
        {
            private const string IntervalsToken = "Intervals";
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                var config = value as DataCompactionConfiguration;
                writer.WriteStartObject();
                writer.WritePropertyName(IntervalsToken);
                serializer.Serialize(writer, config.Intervals);
                writer.WriteEndObject();
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var obj = JObject.Load(reader);
                var intervalsArray = obj.GetValue(IntervalsToken, StringComparison.OrdinalIgnoreCase);
                var intervals = serializer.Deserialize<List<DataIntervalConfiguration>>(intervalsArray.CreateReader());
                return new DataCompactionConfiguration(intervals);
            }

            public override bool CanConvert(Type objectType)
            {
                return objectType == typeof(DataCompactionConfiguration);
            }
        }
    }
}
