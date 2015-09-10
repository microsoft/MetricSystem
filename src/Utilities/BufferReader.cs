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

namespace MetricSystem.Utilities
{
    using System;
    using System.IO;

    /// <summary>
    /// Provides mechanisms for reading variable and fixed-length encoded values from memory.
    /// </summary>
    public sealed unsafe class BufferReader
    {
        private readonly byte* buffer;
        private readonly long bufferSize;

        public BufferReader(byte* buffer, long bufferSize)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException("buffer");
            }

            if (bufferSize <= 0)
            {
                throw new ArgumentOutOfRangeException("bufferSize");
            }

            this.buffer = buffer;
            this.bufferSize = bufferSize;
        }

        /// <summary>
        /// Returns the current number of bytes read from the buffer.
        /// </summary>
        public long BytesRead { get; private set; }

        /// <summary>
        /// Returns a pointer to the current position of the buffer.
        /// </summary>
        public byte* Buffer
        {
            get { return this.buffer + this.BytesRead; }
        }

        /// <summary>
        /// Returns the current remaining size of the buffer.
        /// </summary>
        public long BufferSize
        {
            get { return this.bufferSize - this.BytesRead; }
        }

        public string ReadString()
        {
            int length;
            // Extremely long string values could cause issues here, for now we don't expect that.
            this.BytesRead += ByteConverter.VariableLengthEncoding.ReadInt32(this.Buffer, this.BufferSize, out length);
            var characters = stackalloc char[length];
            for (var pos = 0; pos < length; ++pos)
            {
                ushort c;
                this.BytesRead += ByteConverter.VariableLengthEncoding.ReadUInt16(this.Buffer, this.BufferSize, out c);
                characters[pos] = (char)c;
            }

            return new string(characters, 0, length);
        }

        public short ReadVariableLengthInt16()
        {
            short value;
            this.BytesRead += ByteConverter.VariableLengthEncoding.ReadInt16(this.Buffer, this.BufferSize, out value);
            return value;
        }

        public short ReadFixedLengthInt16()
        {
            if (this.BufferSize < sizeof(short))
            {
                throw new EndOfStreamException("Attempt to read beyond the end of the buffer.");
            }
            var value = *(short*)buffer;
            this.BytesRead += sizeof(short);
            return value;
        }

        public ushort ReadVariableLengthUInt16()
        {
            ushort value;
            this.BytesRead += ByteConverter.VariableLengthEncoding.ReadUInt16(this.Buffer, this.BufferSize, out value);
            return value;
        }

        public ushort ReadFixedLengthUInt16()
        {
            if (this.BufferSize < sizeof(ushort))
            {
                throw new EndOfStreamException("Attempt to read beyond the end of the buffer.");
            }
            var value = *(ushort*)buffer;
            this.BytesRead += sizeof(ushort);
            return value;
        }

        public int ReadVariableLengthInt32()
        {
            int value;
            this.BytesRead += ByteConverter.VariableLengthEncoding.ReadInt32(this.Buffer, this.BufferSize, out value);
            return value;
        }

        public int ReadFixedLengthInt32()
        {
            if (this.BufferSize < sizeof(int))
            {
                throw new EndOfStreamException("Attempt to read beyond the end of the buffer.");
            }
            var value = *(int*)buffer;
            this.BytesRead += sizeof(int);
            return value;
        }

        public uint ReadVariableLengthUInt32()
        {
            uint value;
            this.BytesRead += ByteConverter.VariableLengthEncoding.ReadUInt32(this.Buffer, this.BufferSize, out value);
            return value;
        }

        public uint ReadFixedLengthUInt32()
        {
            if (this.BufferSize < sizeof(uint))
            {
                throw new EndOfStreamException("Attempt to read beyond the end of the buffer.");
            }
            var value = *(uint*)buffer;
            this.BytesRead += sizeof(uint);
            return value;
        }

        public long ReadVariableLengthInt64()
        {
            long value;
            this.BytesRead += ByteConverter.VariableLengthEncoding.ReadInt64(this.Buffer, this.BufferSize, out value);
            return value;
        }

        public long ReadFixedLengthInt64()
        {
            if (this.BufferSize < sizeof(long))
            {
                throw new EndOfStreamException("Attempt to read beyond the end of the buffer.");
            }
            var value = *(long*)buffer;
            this.BytesRead += sizeof(long);
            return value;
        }

        public ulong ReadVariableLengthUInt64()
        {
            ulong value;
            this.BytesRead += ByteConverter.VariableLengthEncoding.ReadUInt64(this.Buffer, this.BufferSize, out value);
            return value;
        }

        public ulong ReadFixedLengthUInt64()
        {
            if (this.BufferSize < sizeof(ulong))
            {
                throw new EndOfStreamException("Attempt to read beyond the end of the buffer.");
            }
            var value = *(ulong*)buffer;
            this.BytesRead += sizeof(ulong);
            return value;
        }
    }
}
