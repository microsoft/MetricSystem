// ---------------------------------------------------------------------
// <copyright file="Key.cs" company="Microsoft">
//       Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    using MetricSystem.Utilities;

    public sealed class Key : IEquatable<Key>, IComparable<Key>, ICloneable
    {
        /// <summary>
        /// The 'Wildcard' value denotes a value which works in two ways:
        /// 1) On the left hand side of a matching constraint it will match anything.
        /// 2) When used as the value of a dimension it means "none of the dimension literals".
        /// </summary>
        public const uint WildcardDimensionValue = uint.MaxValue;

        private static readonly Key[] WildcardKeys =
        {
            new Key(new uint[0]),
            new Key(new[] {WildcardDimensionValue}),
            new Key(new[] {WildcardDimensionValue, WildcardDimensionValue}),
            new Key(new[] {WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue}),
            new Key(new[] {WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue}),
            new Key(new[] {WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue}),
            new Key(new[] {WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue}),
            new Key(new[] {WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue, WildcardDimensionValue}),
        };

        public static Key GetWildcardKey(DimensionSet dimensionSet)
        {
            if (dimensionSet == null)
            {
                throw new ArgumentNullException("dimensionSet");
            }

            var depth = dimensionSet.dimensions.Length;
            if (depth < WildcardKeys.Length)
            {
                return WildcardKeys[dimensionSet.dimensions.Length];
            }

            var key = new Key(new uint[depth]);
            for (var d = 0; d < depth; ++d)
            {
                key[d] = WildcardDimensionValue;
            }

            return key;
        }

        /// <summary>
        /// Determines the maximum number of values an individual key dimension may have.
        /// </summary>
        public const uint MaxDimensionValues = 1 << 20; // ~1m values, quite a lot really!

        internal readonly uint[] Values;

        #region indexers
        internal uint this[uint index]
        {
            get { return this.Values[index]; }
            set { this.Values[index] = value; }
        }

        internal uint this[int index]
        {
            get { return this.Values[index]; }
            set { this.Values[index] = value; }
        }

        internal uint this[long index]
        {
            get { return this.Values[index]; }
            set { this.Values[index] = value; }
        }

        internal uint this[ulong index]
        {
            get { return this.Values[index]; }
            set { this.Values[index] = value; }
        }
        #endregion

        internal int Length { get { return this.Values.Length; } }

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="values">Values provided.</param>
        internal Key(uint[] values)
        {
            this.Values = values;
        }

        /// <summary>
        /// Copy ctor.
        /// </summary>
        /// <param name="other">Key to copy data from.</param>
        internal Key(Key other)
        {
            this.Values = new uint[other.Length];
            other.CopyTo(this);
        }

        public override int GetHashCode()
        {
            if (this.Length == 0)
            {
                return 0;
            }

            uint hash = 19;
            for (int i = 1; i < this.Length; ++i)
            {
                hash = hash * 31 + this[i];
            }

            return (int)hash;
        }

        internal void CopyTo(Key otherKey)
        {
            Buffer.BlockCopy(this.Values, 0, otherKey.Values, 0, this.Length * sizeof(uint));
        }

        public object Clone()
        {
            var newKey = new Key(new uint[this.Length]);
            this.CopyTo(newKey);

            return newKey;
        }

        public int CompareTo(Key other)
        {
            return Compare(this, other);
        }

        public override string ToString()
        {
            return '[' + string.Join(", ", this.Values) + ']';
        }

        /// <summary>
        /// Returns true if this Key would match a given other key. This key may have wildcard values which, if present,
        /// will match any value in the 'other' key (wildcard or otherwise). If the 'other' key has wildcard values for
        /// a dimension but this key does not then this will NOT be considered a match.
        /// </summary>
        /// <param name="other"></param>
        /// <returns>True if this key is a match against the provided other key.</returns>
        public bool Matches(Key other)
        {
            if (other == null || this.Length != other.Length)
            {
                return false;
            }

            for (int i = 0; i < this.Length; ++i)
            {
                if (this[i] != WildcardDimensionValue && this[i] != other[i])
                {
                    return false;
                }
            }

            return true;
        }

        public static int Compare(Key left, Key right)
        {
            if (object.ReferenceEquals(left, null))
            {
                return (object.ReferenceEquals(right, null) ? 0 : -1);
            }
            if (object.ReferenceEquals(right, null))
            {
                return 1;
            }

            // Users of the Key class should only compare Keys across the same dimension set. This check is here
            // to catch any logic fault where key comparison is somehow being done improperly.
            if (left.Length != right.Length)
            {
                throw new InvalidOperationException("Cannot compare keys of differing length.");
            }

            for (var i = 0; i < left.Length; ++i)
            {
                if (left[i] < right[i])
                {
                    return -1;
                }
                if (left[i] > right[i])
                {
                    return 1;
                }
            }

            return 0;
        }

        internal static unsafe int CompareSized(byte* left, byte* right, int keyLength)
        {
            return CompareSized((uint*)left, (uint*)right, keyLength);
        }

        // TODO: Consider comparing larger chunks of memory when possible?
        internal static unsafe int CompareSized(uint* left, uint* right, int keyLength)
        {
            for (var i = 0; i < keyLength; ++i)
            {
                if (*(left + i) < *(right + i))
                {
                    return -1;
                }
                if (*(left + i) > *(right + i))
                {
                    return 1;
                }
            }

            return 0;
        }

        internal void Serialize(Stream writeStream)
        {
            var buffer = new byte[sizeof(uint)];
            for (var i = 0; i < this.Length; ++i)
            {
                ByteConverter.FixedLengthEncoding.Encode(this[i], buffer, writeStream);
            }
        }

        public bool Equals(Key other)
        {
            return Compare(this, other) == 0;
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as Key);
        }

        public static bool operator ==(Key left, Key right)
        {
            return Compare(left, right) == 0;
        }

        public static bool operator !=(Key left, Key right)
        {
            return Compare(left, right) != 0;
        }

        public static bool operator <(Key left, Key right)
        {
            return Compare(left, right) == -1;
        }

        public static bool operator <=(Key left, Key right)
        {
            return Compare(left, right) != 1;
        }

        public static bool operator >(Key left, Key right)
        {
            return Compare(left, right) == 1;
        }
        public static bool operator >=(Key left, Key right)
        {
            return Compare(left, right) != -1;
        }
    }
}
