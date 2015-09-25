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
    using System.Text;

    using Bond.IO;
    using Bond.Protocols;

    using Microsoft.IO;

    /// <summary>
    /// Writer which converts and writes bytes directly to the underlying stream with GC-optimized intermediate buffering
    /// </summary>
    public sealed class WriterStream : IOutputStream, IDisposable
    {
        private readonly Stream innerStream;

        /// <summary>
        /// Intermediate recycled stream which can be used to buffer conversions if necessary.
        /// </summary>
        private readonly MemoryStream intermediateBufferStream;

        public WriterStream(Stream innerStream)
            : this(innerStream, null) { }

        public WriterStream(Stream innerStream, RecyclableMemoryStreamManager memStreamManager)
        {
            if (innerStream == null || !innerStream.CanWrite)
            {
                throw new ArgumentException("input stream must be writable");
            }

            this.innerStream = innerStream;

            // our buffer must be able to hold at least any primitive (sizeof ulong)
            this.intermediateBufferStream = memStreamManager != null
                                                ? memStreamManager.GetStream("BondWriterStream", sizeof(ulong))
                                                : new MemoryStream(sizeof(ulong));
        }

        public void Dispose()
        {
            if (this.intermediateBufferStream != null)
            {
                this.intermediateBufferStream.Dispose();
            }
        }

        public long Position
        {
            get { return this.innerStream.Position; }
            set { this.innerStream.Position = value; }
        }

        public void WriteBytes(ArraySegment<byte> data)
        {
            this.innerStream.Write(data.Array, data.Offset, data.Count);
        }

        public void WriteDouble(double value)
        {
            ByteConverter.FixedLengthEncoding.Encode(value, this.intermediateBufferStream.GetBuffer(), this.innerStream);
        }

        public void WriteFloat(float value)
        {
            ByteConverter.FixedLengthEncoding.Encode(value, this.intermediateBufferStream.GetBuffer(), this.innerStream);
        }

        /// <summary>
        /// Write a string to the stream.
        /// </summary>
        /// <param name="encoding">Encoding to use.</param>
        /// <param name="value">String to write.</param>
        public void WriteString(Encoding encoding, string value)
        {
            var size = encoding.GetByteCount(value);
            this.WriteString(encoding, value, size);
        }

        public void WriteString(Encoding encoding, string value, int size)
        {
            if (this.intermediateBufferStream.Capacity < size)
            {
                this.intermediateBufferStream.Capacity = size;
            }

            encoding.GetBytes(value, 0, size, this.intermediateBufferStream.GetBuffer(), 0);
            this.innerStream.Write(this.intermediateBufferStream.GetBuffer(), 0, size);
        }

        public void WriteUInt16(ushort value)
        {
            ByteConverter.FixedLengthEncoding.Encode(value, this.intermediateBufferStream.GetBuffer(), this.innerStream);
        }

        public void WriteUInt32(uint value)
        {
            ByteConverter.FixedLengthEncoding.Encode(value, this.intermediateBufferStream.GetBuffer(), this.innerStream);
        }

        public void WriteInt64(long value)
        {
            ByteConverter.FixedLengthEncoding.Encode(value, this.intermediateBufferStream.GetBuffer(), this.innerStream);
        }

        public void WriteUInt64(ulong value)
        {
            ByteConverter.FixedLengthEncoding.Encode(value, this.intermediateBufferStream.GetBuffer(), this.innerStream);
        }

        public void WriteUInt8(byte value)
        {
            this.innerStream.WriteByte(value);
        }

        public void WriteVarUInt16(ushort value)
        {
            ByteConverter.VariableLengthEncoding.Write(value, this.innerStream);
        }

        public void WriteVarUInt32(uint value)
        {
            ByteConverter.VariableLengthEncoding.Write(value, this.innerStream);
        }

        public void WriteVarUInt64(ulong value)
        {
            ByteConverter.VariableLengthEncoding.Write(value, this.innerStream);
        }

        /// <summary>
        /// Friendly wrapper to create a CBWriter (tagged, variable-length encoding) out of the input stream
        /// </summary>
        public CompactBinaryWriter<WriterStream> CreateCompactBinaryWriter()
        {
            return new CompactBinaryWriter<WriterStream>(this);
        }

        /// <summary>
        /// Friendly wrapper to create a SimpleWriter (untagged, standard encoding) out of the input stream
        /// </summary>
        public SimpleBinaryWriter<WriterStream> CreateSimpleBinaryWriter()
        {
            return new SimpleBinaryWriter<WriterStream>(this);
        }
    }
}
