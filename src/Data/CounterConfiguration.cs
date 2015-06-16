// ---------------------------------------------------------------------
// <copyright file="CounterConfiguration.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    using MetricSystem.Configuration;

    using Newtonsoft.Json;

    /// <summary>
    /// Represents configuration details used to create counters.
    /// </summary>
    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public sealed class CounterConfiguration : ConfigurationBase
    {
        [JsonProperty(Required = Required.Always, ObjectCreationHandling = ObjectCreationHandling.Reuse)]
        internal readonly Dictionary<string, Counter> Counters =
            new Dictionary<string, Counter>(StringComparer.OrdinalIgnoreCase);

        [JsonProperty]
        internal readonly Dictionary<string, DimensionSet> DimensionSets =
            new Dictionary<string, DimensionSet>(StringComparer.OrdinalIgnoreCase);

        public override bool Validate()
        {
            var valid = true;
            foreach (var kvp in this.Counters)
            {
                var counterName = kvp.Key;
                var config = kvp.Value;

                if (!Protocol.IsValidCounterName(counterName))
                {
                    this.LogError("Invalid counter name '{0}'", counterName);
                    valid = false;
                }

                if (!string.IsNullOrEmpty(config.DimensionSet) && !this.DimensionSets.ContainsKey(config.DimensionSet))
                {
                    this.LogError("Invalid dimension set '{0}' specifieid for counter '{1}'",
                                  config.DimensionSet, counterName);
                    valid = false;
                }

                if (config.Type < CounterType.HitCount || config.Type >= CounterType.Unknown)
                {
                    this.LogError("Invalid type specified for counter '{0}'", counterName);
                    valid = false;
                }

                if (config.Rounding < CounterRounding.None || config.Rounding >= CounterRounding.Unknown)
                {
                    this.LogError("Unknown rounding type for counter '{0}'.", counterName);
                    valid = false;
                }

                switch (config.Rounding)
                {
                case CounterRounding.ByteCount:
                case CounterRounding.SignificantDigits:
                    if (config.RoundingFactor <= 0)
                    {
                        this.LogError("Rounding factor must be greater than zero for counter '{0}'.", counterName);
                        valid = false;
                    }
                    if (config.Type != CounterType.Histogram)
                    {
                        this.LogError("Rounding can only be applied to histograms for counter '{0}'.", counterName);
                        valid = false;
                    }
                    break;
                }
            }

            return valid;
        }

        public async Task Apply(DataManager dataManager)
        {
            foreach (var kvp in this.Counters)
            {
                var counterName = kvp.Key;
                var config = kvp.Value;

                Data.Counter counter;
                DimensionSet dimensionSet;
                if (string.IsNullOrEmpty(config.DimensionSet))
                {
                    dimensionSet = DimensionSet.Empty;
                }
                else
                {
                    dimensionSet = this.DimensionSets[config.DimensionSet];
                }

                switch (config.Type)
                {
                case CounterType.HitCount:
                    counter = await dataManager.CreateHitCounter(counterName, dimensionSet);
                    break;

                case CounterType.Histogram:
                    counter = await dataManager.CreateHistogramCounter(counterName, dimensionSet);
                    break;
                default:
                    throw new ArgumentException("Unknown counter type.");
                }

                switch (config.Rounding)
                {
                case CounterRounding.ByteCount:
                case CounterRounding.SignificantDigits:
                    if (config.RoundingFactor <= 0)
                    {
                        throw new InvalidOperationException("Rounding factor must be greater than zero.");
                    }
                    if (config.Type != CounterType.Histogram)
                    {
                        throw new InvalidOperationException("Rounding can only be applied to histograms.");
                    }

                    var histogramCounter = counter as HistogramCounter;
                    if (histogramCounter == null)
                    {
                        throw new InvalidOperationException("Attempt to apply rounding to non-histogram counter,");
                    }
                    histogramCounter.Rounding = config.Rounding;
                    histogramCounter.RoundingFactor = config.RoundingFactor;
                    break;
                }

                if (config.Aggregate)
                {
                    counter.Flags |= CounterFlags.AggregationEnabled;
                }
            }
        }

        [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
        internal sealed class Counter
        {
            [JsonProperty]
            public string DimensionSet;

            [JsonProperty(Required = Required.Always)]
            public CounterType Type;

            [JsonProperty]
            public CounterRounding Rounding;

            [JsonProperty]
            public int RoundingFactor = 0;

            [JsonProperty]
            public bool Aggregate = true;
        }

        #region Enumeration handling
        [JsonConverter(typeof(CounterTypeConverter))]
        internal enum CounterType
        {
            HitCount,
            Histogram,
            Unknown,
        };

        private sealed class CounterTypeConverter : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                writer.WriteValue(((CounterType)value).ToString());
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
                                            JsonSerializer serializer)
            {
                CounterType readType;
                if (!Enum.TryParse(reader.Value.ToString(), true, out readType))
                {
                    readType = CounterType.Unknown;
                }

                return readType;
            }

            public override bool CanConvert(Type objectType)
            {
                return objectType == typeof(CounterType);
            }
        }

        internal sealed class CounterRoundingConverter : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                writer.WriteValue(((CounterRounding)value).ToString());
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
                                            JsonSerializer serializer)
            {
                CounterRounding readType;
                if (!Enum.TryParse(reader.Value.ToString(), true, out readType))
                {
                    readType = CounterRounding.Unknown;
                }

                return readType;
            }

            public override bool CanConvert(Type objectType)
            {
                return objectType == typeof(CounterRounding);
            }
        }
        #endregion
    }
}
