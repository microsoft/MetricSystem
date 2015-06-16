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
    using System.Collections.Generic;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    [JsonObject(MemberSerialization = MemberSerialization.OptIn), JsonConverter(typeof(Converter))]
    public sealed class DataIntervalConfiguration
    {
        private const string ForeverDuration = "forever";

        /// <summary>
        /// Interval for individual segments of data.
        /// </summary>
        [JsonProperty]
        public TimeSpan Interval { get; private set; }

        /// <summary>
        /// Total duration of time for the provided interval.
        /// </summary>
        [JsonProperty]
        public TimeSpan Duration { get; private set; }

        /// <summary>
        /// Ctor for configuring the duration of each bucket and the total
        /// time in hours to maintain at this quantum.
        /// </summary>
        /// <param name="interval">Duration of data held within each bucket</param>
        /// <param name="duration">Total duration to keep buckets at the supplied bucket time span</param>
        public DataIntervalConfiguration(TimeSpan interval, TimeSpan duration)
        {
            if (interval <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException("interval");
            }

            if (duration.Ticks % interval.Ticks != 0 && duration != TimeSpan.MaxValue)
            {
                throw new ArgumentException("Invalid bucket configuration");
            }

            this.Interval = interval;
            this.Duration = duration;
        }

        public DateTime ConvertToAlternateBucketDuration(DateTime timestamp)
        {
            return (timestamp - TimeSpan.FromTicks(timestamp.Ticks % this.Interval.Ticks));
        }

        public override string ToString()
        {
            return string.Format("{0} ({1})", this.Interval,
                                 this.Duration == TimeSpan.MaxValue
                                     ? ForeverDuration
                                     : this.Duration.ToString());
        }
        private sealed class Converter : JsonConverter
        {
            private const string IntervalToken = "Interval";
            private const string DurationToken = "Duration";

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                var config = value as DataIntervalConfiguration;
                writer.WriteStartObject();
                writer.WritePropertyName(IntervalToken);
                writer.WriteValue(config.Interval.ToString());
                writer.WritePropertyName(DurationToken);
                writer.WriteValue(config.Duration == TimeSpan.MaxValue ? ForeverDuration : config.Duration.ToString());
                writer.WriteEndObject();
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
                                            JsonSerializer serializer)
            {
                var jObject = JObject.Load(reader);
                var intervalToken = jObject.GetValue(IntervalToken, StringComparison.OrdinalIgnoreCase);
                var durationToken = jObject.GetValue(DurationToken, StringComparison.OrdinalIgnoreCase);

                var interval = TimeSpan.Parse(intervalToken.Value<string>());

                var durationString = durationToken.Value<string>();
                var duration = string.Equals(durationString, ForeverDuration, StringComparison.OrdinalIgnoreCase)
                                   ? TimeSpan.MaxValue
                                   : TimeSpan.Parse(durationString);

                return new DataIntervalConfiguration(interval, duration);
            }

            public override bool CanConvert(Type objectType)
            {
                return objectType == typeof(DataIntervalConfiguration);
            }
        }
    }
    internal sealed class CompactionSet<TInternal>
        where TInternal : class, IInternalData, new()
    {
        internal DataIntervalConfiguration TargetIntervalConfiguration;
        internal List<DataBucket<TInternal>> BucketSet;
        internal TimeSpan SourceBucketDuration;
        internal DateTime TargetTimestamp;

        internal CompactionSet()
        {
            this.BucketSet = new List<DataBucket<TInternal>>();
        }
    }
}
