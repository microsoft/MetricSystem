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
