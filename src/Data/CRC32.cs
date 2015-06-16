// ---------------------------------------------------------------------
// <copyright file="CRC32.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System.Diagnostics.CodeAnalysis;

    /// <remarks>
    /// Assembled by examination of http://en.wikipedia.org/wiki/Cyclic_redundancy_check
    /// </remarks>
    internal static class CRC32
    {
        private const uint Seed = 0xffffffff;
        private const uint Polynomial = 0xedb88320;

        private static readonly uint[] LookupTable;

        /// <summary>
        /// Initializez the crc32 object.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline")]
        static CRC32()
        {
            LookupTable = new uint[256];
            for (uint i = 0; i < LookupTable.Length; ++i)
            {
                var temp = i;
                for (var j = 8; j > 0; --j)
                {
                    if ((temp & 1) == 1)
                    {
                        temp = ((temp >> 1) ^ Polynomial);
                    }
                    else
                    {
                        temp >>= 1;
                    }
                }
                LookupTable[i] = temp;
            }
        }

        public static unsafe uint Compute(byte[] data, int offset, long length)
        {
            fixed (byte* buffer = data)
            {
                return Compute(buffer + offset, length);
            }
        }

        public static unsafe uint Compute(byte[] data, long length)
        {
            fixed (byte* buffer = data)
            {
                return Compute(buffer, length);
            }
        }

        public static unsafe uint Compute(byte* data, long length)
        {
            var crc = Seed;
            for (var i = 0; i < length; ++i)
            {
                crc = (crc >> 8) ^ LookupTable[((byte)crc & 0xff) ^ data[i]];
            }
            return ~crc;
        }
    }
}
