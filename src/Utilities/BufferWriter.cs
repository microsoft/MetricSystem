// ---------------------------------------------------------------------
// <copyright file="BufferWriter.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Utilities
{
    using System;
    using System.IO;

    /// <summary>
    /// Provides a wrapper for writing strings and fixed or variable length encoded values to a stream. Tracks the
    /// total bytes written as it goes.
    /// </summary>
    public sealed class BufferWriter
    {
        // Used for FLE encoding and subsequent writes.
        private readonly byte[] buffer = new byte[sizeof(double)];
        private readonly Stream stream;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="stream">Stream to write to.</param>
        public BufferWriter(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            this.stream = stream;
        }

        /// <summary>
        /// Returns the current number of bytes written to the stream.
        /// </summary>
        public long BytesWritten { get; private set; }

        /// <summary>
        /// Write a single byte to the stream.
        /// </summary>
        /// <param name="b">Byte value to write.</param>
        public void WriteByte(byte b)
        {
            this.stream.WriteByte(b);
            ++this.BytesWritten;
        }

        /// <summary>
        /// Write a string to the stream. The string is prefixed with a VLE-encoded length and each character is
        /// also written as a VLE short value.
        /// </summary>
        /// <param name="value">The string to write.</param>
        public unsafe void WriteString(string value)
        {
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            var length = ByteConverter.VariableLengthEncoding.Write(value.Length, this.stream);
            fixed (char* s = value)
            {
                for (var i = 0; i < value.Length; ++i)
                {
                    length += ByteConverter.VariableLengthEncoding.Write(*((short*)s + i), this.stream);
                }
            }

            this.BytesWritten += length;
        }

        public void WriteVariableLengthInt16(short value)
        {
            this.BytesWritten += ByteConverter.VariableLengthEncoding.Write(value, this.stream);
        }

        public void WriteFixedLengthInt16(short value)
        {
            ByteConverter.FixedLengthEncoding.Encode(value, this.buffer, this.stream);
            this.BytesWritten += sizeof(short);
        }

        public void WriteVariableLengthUInt16(ushort value)
        {
            this.BytesWritten += ByteConverter.VariableLengthEncoding.Write(value, this.stream);
        }

        public void WriteFixedLengthUInt16(ushort value)
        {
            ByteConverter.FixedLengthEncoding.Encode(value, this.buffer, this.stream);
            this.BytesWritten += sizeof(ushort);
        }

        public void WriteVariableLengthInt32(int value)
        {
            this.BytesWritten += ByteConverter.VariableLengthEncoding.Write(value, this.stream);
        }

        public void WriteFixedLengthInt32(int value)
        {
            ByteConverter.FixedLengthEncoding.Encode(value, this.buffer, this.stream);
            this.BytesWritten += sizeof(int);
        }

        public void WriteVariableLengthUInt32(uint value)
        {
            this.BytesWritten += ByteConverter.VariableLengthEncoding.Write(value, this.stream);
        }

        public void WriteFixedLengthUInt32(uint value)
        {
            ByteConverter.FixedLengthEncoding.Encode(value, this.buffer, this.stream);
            this.BytesWritten += sizeof(ushort);
        }

        public void WriteVariableLengthInt64(long value)
        {
            this.BytesWritten += ByteConverter.VariableLengthEncoding.Write(value, this.stream);
        }

        public void WriteFixedLengthInt64(long value)
        {
            ByteConverter.FixedLengthEncoding.Encode(value, this.buffer, this.stream);
            this.BytesWritten += sizeof(long);
        }

        public void WriteVariableLengthUInt64(ulong value)
        {
            this.BytesWritten += ByteConverter.VariableLengthEncoding.Write(value, this.stream);
        }

        public void WriteFixedLengthUInt64(ulong value)
        {
            ByteConverter.FixedLengthEncoding.Encode(value, this.buffer, this.stream);
            this.BytesWritten += sizeof(ulong);
        }
    }
}
