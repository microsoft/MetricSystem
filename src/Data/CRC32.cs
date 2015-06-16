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
