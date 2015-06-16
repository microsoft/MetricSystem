// ---------------------------------------------------------------------
// <copyright file="DimensionSet.cs" company="Microsoft">
//       Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Text;

    using MetricSystem.Utilities;

    using Newtonsoft.Json;

    /// <summary>
    /// A DimensionSet manages dimensions for data by providing functions to generate keys for specific dimensions,
    /// and match keys against each other.
    /// </summary>
    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public sealed class DimensionSet : IEquatable<DimensionSet>, ICloneable
    {
        [JsonProperty("Dimensions")]
        internal readonly Dimension[] dimensions;

        private static IEnumerable<Dimension> OrderDimensionsBySize(IEnumerable<Dimension> dimensions)
        {
            return from dimension in dimensions
                   orderby dimension.Count descending
                   select dimension;
        }

        /// <summary>
        /// Create an ad-hoc dimension set from a query request (to facilitate easy normalization/de-duping
        /// of dimensions received as part of the request)
        /// </summary>
        /// <param name="queryParameters"></param>
        /// <returns></returns>
        public static DimensionSet FromQueryParameters(IDictionary<string, string> queryParameters)
        {
            if (queryParameters == null)
            {
                return Empty;
            }


            // build an adhoc dimension set based on the query parameters. Any non-reserved dimension and the "split-by" value
            // goes into the set.
            var filteredParameters = new HashSet<Dimension>();
            foreach (var kvp in queryParameters)
            {
                if (ReservedDimensions.DimensionDimension.Equals(kvp.Key))
                {
                    // keep the target of the "split by" dimension
                    filteredParameters.Add(new Dimension(kvp.Value));
                }
                else if (!Dimension.IsReservedDimension(kvp.Key))
                {
                    filteredParameters.Add(new Dimension(kvp.Key));
                }
            }

            // its ok if the filtered set is empty. We'll just end up with 0 dimensions
            return new DimensionSet(new HashSet<Dimension>(filteredParameters));
        }

        /// <summary>
        /// An empty / zero dimension set.
        /// </summary>
        public static readonly DimensionSet Empty = new DimensionSet(new HashSet<Dimension>());

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="dimensions">Collection of dimensions to be used for keys.</param>
        public DimensionSet(ISet<Dimension> dimensions)
        {
            if (dimensions == null)
            {
                throw new ArgumentNullException("dimensions");
            }
            if (dimensions.Contains(null))
            {
                throw new ArgumentException("null Dimension object in set.", "dimensions");
            }

            this.dimensions = OrderDimensionsBySize(dimensions).ToArray();
        }

        /// <summary>
        /// Shallow copy dimensions from an existing DimensionSet, ordering our dimensions by size of the others.
        /// </summary>
        /// <param name="otherSet">The set to copy dimensions from.</param>
        internal DimensionSet(DimensionSet otherSet)
        {
            this.dimensions = (from dimension in OrderDimensionsBySize(otherSet.dimensions)
                               select new Dimension(dimension)).ToArray();
        }

        internal unsafe DimensionSet(byte* buffer, uint bufferSize)
        {
            long offset = 0;
            PersistedDataReader.CheckRemainingBufferSize(bufferSize, 0, sizeof(int));
            var dimensionCount = *(int*)buffer;
            offset += sizeof(int);

            this.dimensions = new Dimension[dimensionCount];
            for (var i = 0; i < dimensionCount; ++i)
            {
                this.dimensions[i] = new Dimension((buffer + offset), bufferSize - offset);
                offset += this.dimensions[i].SerializedSize;
            }

            this.SerializedSize = offset;
        }

        internal DimensionSet(BufferReader reader)
        {
            try
            {
                var start = reader.BytesRead;
                var dimensionCount = reader.ReadVariableLengthInt32();
                this.dimensions = new Dimension[dimensionCount];
                for (var i = 0; i < dimensionCount; ++i)
                {
                    this.dimensions[i] = new Dimension(reader);
                }
                this.SerializedSize = reader.BytesRead - start;
            }
            catch (EndOfStreamException ex) // VLE throws this for buffer reads.
            {
                throw new PersistedDataException("Buffer is too small or corrupted.", ex);
            }
        }

        /// <summary>
        /// Dimensions for the key. The enumeration is ordered in the same way that keys are generated.
        /// </summary>
        public IEnumerable<Dimension> Dimensions
        {
            get
            {
                return this.dimensions;
            }
        }

        /// <summary>
        /// Determines if the other DimensionSet has the same dimensions (in any order) as this one.
        /// </summary>
        /// <param name="other">DimensionSet to compare against.</param>
        /// <returns>True if the same dimensions are represented in both sets.</returns>
        public bool Equals(DimensionSet other)
        {
            if (other == null)
            {
                return false;
            }

            if (this.dimensions.Length != other.dimensions.Length)
            {
                return false;
            }

            return this.dimensions.All(dim => other.dimensions.Any(dim.Equals));
        }

        /// <summary>
        /// Generate a Key object representing the provided dimension values.
        /// </summary>
        /// <param name="dimensionValues">Key/value collection of dimensions.</param>
        /// <returns>The generated key.</returns>
        public Key CreateKey(IDictionary<string, string> dimensionValues)
        {
            bool unused;
            return this.CreateKey(dimensionValues, out unused);
        }

        /// <summary>
        /// Generate a Key object representing the provided dimension values.
        /// </summary>
        /// <param name="dimensionValues">Key/value collection of dimensions.</param>
        /// <param name="allDimensionsProvided">Set if all dimensions known to the key were provided.</param>
        /// <returns>The generated key.</returns>
        [SuppressMessage("Microsoft.Design", "CA1021:AvoidOutParameters", MessageId = "1#")]
        internal unsafe Key CreateKey(IDictionary<string, string> dimensionValues, out bool allDimensionsProvided)
        {
            var indexedDimensions = stackalloc uint[this.dimensions.Length];

            allDimensionsProvided = this.PopulateKeyArray(dimensionValues, indexedDimensions);

            var dims = new uint[this.dimensions.Length];
            for (var i = 0; i < this.dimensions.Length; ++i)
            {
                dims[i] = indexedDimensions[i];
            }

            return new Key(dims);
        }

        /// <summary>
        /// Populate an array of key data using the provided dimension values.
        /// </summary>
        /// <param name="dimensionValues">Key/value collection of dimensions.</param>
        /// <param name="indexedDimensions">Array to populate.</param>
        /// <returns>True if all dimensions were represented in <see cref="dimensionValues" /></returns>
        internal unsafe bool PopulateKeyArray(IDictionary<string, string> dimensionValues, uint* indexedDimensions)
        {
            if (dimensionValues == null)
            {
                throw new ArgumentNullException("dimensionValues");
            }

            var matchedDimensions = 0;
            for (int i = 0; i < this.dimensions.Length; i++)
            {
                var dim = this.dimensions[i];
                string matchValue;
                if (dimensionValues.TryGetValue(dim.Name, out matchValue))
                {
                    indexedDimensions[i] = dim.StringToIndex(matchValue);
                    ++matchedDimensions;
                }
                else
                {
                    indexedDimensions[i] = Key.WildcardDimensionValue;
                }
            }

            return (matchedDimensions == this.dimensions.Length);
        }

        /// <summary>
        /// Convert a key to a string representation of its dimension:value pairs.
        /// </summary>
        /// <param name="key">The key to convert</param>
        /// <returns>A comma-separated list of dimension:value pairs.</returns>
        public string KeyToString(Key key)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            if (this.dimensions.Length == 0)
            {
                return string.Empty;
            }

            var sb = new StringBuilder();
            for (var i = 0; i < this.dimensions.Length; ++i)
            {
                if (sb.Length > 0) sb.Append(", ");
                sb.AppendFormat("{0}:{1}", this.dimensions[i].Name, this.dimensions[i].IndexToString(key[i]));
            }

            return sb.ToString();
        }

        public DimensionSpecification KeyToDimensionSpecification(Key key)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            var dimensionSpec = new DimensionSpecification();
            for (var i = 0; i < this.dimensions.Length; ++i)
            {
                dimensionSpec.Add(this.dimensions[i].Name, this.dimensions[i].IndexToString(key[i]));
            }

            return dimensionSpec;
        }

        /// <summary>
        /// Provides the integer offset within a key of the named dimension.
        /// </summary>
        /// <param name="dimensionName">The dimension.</param>
        /// <returns>The offset of the dimension.</returns>
        internal int GetOffsetOfDimension(string dimensionName)
        {
            int offset = Array.FindIndex(this.dimensions,
                dim => string.Equals(dim.Name, dimensionName,
                    StringComparison.OrdinalIgnoreCase));

            if (offset < 0)
            {
                throw new KeyNotFoundException(dimensionName);
            }

            return offset;
        }

        /// <summary>
        /// Provides the value of a dimension at the provided offset.
        /// </summary>
        /// <param name="key">The key to use.</param>
        /// <param name="offset">The offset within the key of the desired dimension value.</param>
        /// <returns>The dimension value.</returns>
        internal string GetDimensionValueAtOffset(Key key, int offset)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            if (offset < 0 || offset >= this.dimensions.Length)
            {
                throw new ArgumentOutOfRangeException("offset");
            }

            return this.dimensions[offset].IndexToString(key[offset]);
        }

        internal void Write(BufferWriter writer)
        {
            var start = writer.BytesWritten;
            writer.WriteVariableLengthInt32(this.dimensions.Length);
            foreach (var dim in this.dimensions)
            {
                dim.Write(writer);
            }

            this.SerializedSize = writer.BytesWritten - start;
        }

        public object Clone()
        {
            var clonedSet = new DimensionSet(new HashSet<Dimension>(this.dimensions));
            return clonedSet;
        }

        internal bool Validate(Key key)
        {
            for (var i = 0; i < this.dimensions.Length; ++i)
            {
                if (!this.dimensions[i].ValidateIndex(key[i]))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Serialized size in bytes of the entire DimensionSet.
        /// </summary>
        internal long SerializedSize { get; private set; }
    }
}
