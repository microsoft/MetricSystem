// ---------------------------------------------------------------------
// <copyright file="Dimension.cs" company="Microsoft">
//       Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    using MetricSystem;
    using MetricSystem.Utilities;

    /// <summary>
    /// A single dimension used to filter data.
    /// </summary>
    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    [JsonConverter(typeof(Converter))]
    public sealed class Dimension : IEquatable<Dimension>
    {
        internal static readonly HashSet<string> ReservedDimensionNames =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                ReservedDimensions.MachineDimension,
                ReservedDimensions.EnvironmentDimension,
                ReservedDimensions.DatacenterDimension,
                ReservedDimensions.StartTimeDimension,
                ReservedDimensions.EndTimeDimension,
                ReservedDimensions.PercentileDimension,
                ReservedDimensions.AggregateSamplesDimension,
                ReservedDimensions.DimensionDimension,
            };

        private readonly List<string> indexToValues;

        private readonly Dictionary<string, uint> valuesToIndex =
            new Dictionary<string, uint>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="name">Name of the dimension.</param>
        public Dimension(string name) : this(name, null)
        {
        }

        public Dimension(string name, ISet<string> allowedValues)
        {
            if (string.IsNullOrWhiteSpace(name) || ReservedDimensionNames.Contains(name))
            {
                throw new ArgumentException("Invalid dimension name", "name");
            }

            this.Name = name.MaybeIntern();
            this.AllowedValues = allowedValues;
            this.indexToValues = new List<string>();
        }

        /// <summary>
        /// Create a shallow (settings only) copy of an existing dimension.
        /// </summary>
        /// <param name="otherDimension">The dimension to copy.</param>
        internal Dimension(Dimension otherDimension)
            : this(otherDimension.Name, otherDimension.AllowedValues)
        {
        }

        internal unsafe Dimension(byte* buffer, long bufferSize)
        {
            string value;
            var offset = PersistedDataReader.ReadLegacyStringValue(buffer, bufferSize, out value);
            this.Name = value;

            PersistedDataReader.CheckRemainingBufferSize(bufferSize, offset, sizeof(int));
            var valueCount = *(int*)(buffer + offset);
            offset += sizeof(int);

            this.indexToValues = new List<string>(valueCount);
            while (valueCount > 0)
            {
                offset += PersistedDataReader.ReadLegacyStringValue((buffer + offset), bufferSize - offset,
                                                                    out value);

                if (this.valuesToIndex.ContainsKey(value))
                {
                    throw new PersistedDataException(string.Format("Duplicate dimension value {0} in {1}", value,
                                                                   this.Name));
                }
                this.valuesToIndex.Add(value, (uint)this.indexToValues.Count);
                this.indexToValues.Add(value);
                --valueCount;
            }

            this.SerializedSize = offset;
        }

        internal Dimension(BufferReader reader)
        {
            var start = reader.BytesRead;
            this.Name = reader.ReadString();

            var valueCount = reader.ReadVariableLengthInt32();
            this.indexToValues = new List<string>(valueCount);
            for (var i = 0; i < valueCount; ++i)
            {
                var value = reader.ReadString();
                if (this.valuesToIndex.ContainsKey(value))
                {
                    throw new PersistedDataException(string.Format("Duplicate dimension value {0} in {1}", value,
                                                                   this.Name));
                }
                this.valuesToIndex.Add(value, (uint)this.indexToValues.Count);
                this.indexToValues.Add(value);
            }

            this.SerializedSize = reader.BytesRead - start;
        }

        internal void Write(BufferWriter writer)
        {
            var start = writer.BytesWritten;
            writer.WriteString(this.Name);
            writer.WriteVariableLengthInt32(this.indexToValues.Count);
            foreach (var value in this.indexToValues)
            {
                writer.WriteString(value);
            }

            this.SerializedSize = writer.BytesWritten - start;
        }

        /// <summary>
        /// Name of this dimension.
        /// </summary>
        [JsonProperty(NameToken, Required = Required.Always)]
        public string Name { get; private set; }

        public const string NameToken = "Name";

        /// <summary>
        /// If non-null specifies the set of values which are treated as non-wildcard for this dimension.
        /// </summary>
        [JsonProperty(AllowedValuesToken)]
        public ISet<string> AllowedValues { get; private set; }

        public const string AllowedValuesToken = "AllowedValues";

        /// <summary>
        /// Indicates the PriorityLevel of the dimension. It will be used for throttling
        /// events at the source machine. The lower the number, the higher the PriorityLevel.
        /// </summary>
        [JsonProperty]
        public int Priority { get; set; }

        /// <summary>
        /// If specified, the dimension is collected for global aggregation if GlobalCounterGroup is configured
        /// for the counter. If not specified, by default, the dimension is only collected and aggregated at 
        /// the environment level
        /// </summary>
        [JsonProperty]
        public bool EnableGlobal { get; set; }

        public bool Equals(Dimension other)
        {
            return other != null && String.Equals(this.Name, other.Name, StringComparison.OrdinalIgnoreCase);
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as Dimension);
        }

        public override int GetHashCode()
        {
            return this.Name.ToLower(CultureInfo.InvariantCulture).GetHashCode();
        }

        public uint StringToIndex(string value)
        {
            uint idx = Key.WildcardDimensionValue;
            if (string.IsNullOrEmpty(value))
            {
                return idx;
            }
            if (this.AllowedValues != null && !this.AllowedValues.Contains(value))
            {
                return idx;
            }

            // Possible future optimization: swap 'IndexToValues' out for a string[] and use interlocked operators to
            // get a safely usable offset. We'd still need locks when the array needs to grow, but a simple doubling
            // algorithm would probably work okay and cost somewhat less.
            lock (this.indexToValues)
            {
                value = value.MaybeIntern();

                if (this.valuesToIndex.TryGetValue(value, out idx))
                {
                    return idx;
                }

                if (this.indexToValues.Count == Key.MaxDimensionValues)
                {
                    throw new InvalidOperationException("Tried to add too many Values to single dimension.");
                }

                idx = (uint)(this.indexToValues.Count);
                this.indexToValues.Add(value);
                this.valuesToIndex.Add(value, idx);
            }

            return idx;
        }

        public string IndexToString(uint idx)
        {
            if (idx == Key.WildcardDimensionValue)
            {
                return string.Empty;
            }

            lock (this.indexToValues)
            {
                return this.indexToValues[(int)idx];
            }
        }

        /// <summary>
        /// Determines whether a dimension name is reserved.
        /// </summary>
        /// <param name="dimension">Dimension name.</param>
        /// <returns>True if the dimension is reserved.</returns>
        public static bool IsReservedDimension(string dimension)
        {
            return ReservedDimensionNames.Contains(dimension);
        }

        /// <summary>
        /// Retrieve all known values for this dimension.
        /// </summary>
        internal IEnumerable<string> Values
        {
            get
            {
                lock (this.indexToValues)
                {
                    return this.indexToValues.ToList();
                }
            }
        }

        /// <summary>
        /// Retrieve all known index/string pairs for this dimension.
        /// </summary>
        internal IEnumerable<KeyValuePair<string, uint>> Pairs
        {
            get
            {
                lock (this.indexToValues)
                {
                    return this.valuesToIndex.ToList();
                }
            }
        }

        /// <summary>
        /// Retrieve the number of known values for the dimension.
        /// </summary>
        internal int Count
        {
            get
            {
                lock (this.indexToValues)
                {
                    return this.indexToValues.Count;
                }
            }
        }

        internal bool ValidateIndex(uint i)
        {
            if (i != Key.WildcardDimensionValue)
            {
                lock (this.indexToValues)
                {
                    if (i >= this.indexToValues.Count)
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Size of this dimension when serialized.
        /// </summary>
        internal long SerializedSize { get; private set; }

        private sealed class Converter : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                var dim = value as Dimension;

                writer.WriteStartObject();
                writer.WritePropertyName(NameToken);
                writer.WriteValue(dim.Name);
                if (dim.AllowedValues != null && dim.AllowedValues.Count > 0)
                {
                    writer.WritePropertyName(AllowedValuesToken);
                    writer.WriteStartArray();
                    foreach (var v in dim.AllowedValues)
                    {
                        writer.WriteValue(v);
                    }
                    writer.WriteEndArray();
                }
                writer.WriteEndObject();
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var obj = JObject.Load(reader);

                var nameToken = obj.GetValue(NameToken, StringComparison.OrdinalIgnoreCase);
                var name = nameToken.Value<string>();
                HashSet<string> allowedValues = null;
                JToken allowedValuesArray;
                if (obj.TryGetValue(AllowedValuesToken, StringComparison.OrdinalIgnoreCase, out allowedValuesArray))
                {
                    allowedValues =
                        new HashSet<string>(serializer.Deserialize<List<string>>(allowedValuesArray.CreateReader()),
                                            StringComparer.OrdinalIgnoreCase);
                }

                return new Dimension(name, allowedValues);
            }

            public override bool CanConvert(Type objectType)
            {
                return objectType == typeof(Dimension);
            }
        }
    }
}
