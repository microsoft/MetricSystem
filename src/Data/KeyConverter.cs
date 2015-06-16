// ---------------------------------------------------------------------
// <copyright file="KeyConverter.cs" company="Microsoft">
//       Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    /// <summary>
    /// Provides a mechanism to convert Key values from a source DimensionSet to its corresponding destination.
    /// </summary>
    internal sealed class KeyConverter
    {
        private const int UnmappedOffset = -1;
        private readonly DimensionSet destination;

        // Maps the destination set offset (index value) to the source set offset
        private readonly int[] offsetMap;
        private readonly DimensionSet source;

        private readonly bool skipConversion;

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="source">Source dimension set.</param>
        /// <param name="destination">Destination dimension set.</param>
        public KeyConverter(DimensionSet source, DimensionSet destination)
        {
            if (object.ReferenceEquals(source, destination))
            {
                this.skipConversion = true;
            }

            this.source = source;
            this.destination = destination;

            this.offsetMap = new int[destination.dimensions.Length];
            for (int d = 0; d < destination.dimensions.Length; ++d)
            {
                this.offsetMap[d] = UnmappedOffset;
                for (int s = 0; s < source.dimensions.Length; ++s)
                {
                    if (destination.dimensions[d].Equals(source.dimensions[s]))
                    {
                        this.offsetMap[d] = s;
                        break;
                    }
                }
            }
        }

        public Key Convert(Key sourceKey)
        {
            if (this.skipConversion)
            {
                return sourceKey;
            }

            var values = new uint[this.offsetMap.Length];

            for (int i = 0; i < this.offsetMap.Length; ++i)
            {
                if (this.offsetMap[i] == UnmappedOffset)
                {
                    values[i] = Key.WildcardDimensionValue;
                    continue;
                }

                Dimension sourceDimension = this.source.dimensions[this.offsetMap[i]];
                Dimension destinationDimension = this.destination.dimensions[i];
                string dimensionValue = sourceDimension.IndexToString(sourceKey.Values[this.offsetMap[i]]);
                values[i] = destinationDimension.StringToIndex(dimensionValue);
            }

            return new Key(values);
        }
    }
}
