// ---------------------------------------------------------------------
// <copyright file="ByteConverter.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Utilities
{
    using System;
    using System.IO;

    public static class ByteConverter
    {
        public static class FixedLengthEncoding
        {
            public static unsafe void Encode(short value, byte[] buffer, Stream stream)
            {
                fixed (byte* val = buffer)
                {
                    *(short*)val = value;
                    stream.Write(buffer, 0, sizeof(short));
                }
            }

            public static unsafe void Encode(ushort value, byte[] buffer, Stream stream)
            {
                fixed (byte* val = buffer)
                {
                    *(ushort*)val = value;
                    stream.Write(buffer, 0, sizeof(ushort));
                }
            }

            public static unsafe void Encode(int value, byte[] buffer, Stream stream)
            {
                fixed (byte* val = buffer)
                {
                    *(int*)val = value;
                    stream.Write(buffer, 0, sizeof(int));
                }
            }

            public static unsafe void Encode(uint value, byte[] buffer, Stream stream)
            {
                fixed (byte* val = buffer)
                {
                    *(uint*)val = value;
                    stream.Write(buffer, 0, sizeof(uint));
                }
            }

            public static unsafe void Encode(long value, byte[] buffer, Stream stream)
            {
                fixed (byte* val = buffer)
                {
                    *(long*)val = value;
                    stream.Write(buffer, 0, sizeof(long));
                }
            }

            public static unsafe void Encode(ulong value, byte[] buffer, Stream stream)
            {
                fixed (byte* val = buffer)
                {
                    *(ulong*)val = value;
                    stream.Write(buffer, 0, sizeof(ulong));
                }
            }

            public static unsafe void Encode(double value, byte[] buffer, Stream stream)
            {
                fixed (byte* val = buffer)
                {
                    *(double*)val = value;
                    stream.Write(buffer, 0, sizeof(double));
                }
            }

            public static unsafe void Encode(float value, byte[] buffer, Stream stream)
            {
                fixed (byte* val = buffer)
                {
                    *(float*)val = value;
                    stream.Write(buffer, 0, sizeof(float));
                }
            }
        }

        /// <summary>
        /// Variable-Length encoding (saves space on small numbers). 
        /// For each byte, use the highest bit to say "more data in the next byte". 
        /// The remaining 7 bits contain that value to be OR'd together for that byte.
        /// </summary>
        /// <remarks>
        /// This is an implementation of ULEB128 encoding (see http://en.wikipedia.org/wiki/LEB128)
        /// </remarks>
        public static class VariableLengthEncoding
        {
            public static readonly int Int8MaxBytes = GetMaxBytesForEncoding(sizeof(byte));
            public static readonly int Int16MaxBytes = GetMaxBytesForEncoding(sizeof(short));
            public static readonly int Int32MaxBytes = GetMaxBytesForEncoding(sizeof(int));
            public static readonly int Int64MaxBytes = GetMaxBytesForEncoding(sizeof(long));

            public static uint Write(ushort value, Stream stream)
            {
                return Write((ulong)value, stream);
            }

            public static uint Write(uint value, Stream stream)
            {
                return Write((ulong)value, stream);
            }

            public static uint Write(int value, Stream stream)
            {
                return Write((ulong)value, stream);
            }

            public static uint Write(long value, Stream stream)
            {
                return Write((ulong)value, stream);
            }

            public static unsafe uint Write(ulong value, Stream stream)
            {
                var writeBuffer = stackalloc byte[Int64MaxBytes];
                var bytesWritten = Write(value, writeBuffer, Int64MaxBytes);
                for (var i = 0; i < bytesWritten; ++i)
                {
                    stream.WriteByte(writeBuffer[i]);
                }

                return bytesWritten;
            }

            public static unsafe uint Write(ulong value, byte* buffer, long bufferSize)
            {
                var current = value;
                uint bytesWritten = 0;

                do
                {
                    var val = (byte)(current & 0x7F);
                    current >>= 7;

                    if (current > 0)
                    {
                        val |= 0x80;
                    }

                    if (bytesWritten >= bufferSize)
                    {
                        throw new InvalidOperationException("Buffer space is insufficient to write value.");
                    }

                    buffer[bytesWritten] = val;
                    bytesWritten++;
                } while (current > 0);

                return bytesWritten;
            }

            public static int GetMaxBytesForEncoding(int originalSizeInBytes)
            {
                // we use 7 out of every 8 bits. So round-up(8/7 * size)
                return 1 + (originalSizeInBytes * 8) / 7;
            }

            public static short ReadInt16(Stream stream)
            {
                return (short)ReadUInt16(stream);
            }

            public static ushort ReadUInt16(Stream stream)
            {
                return (ushort)Decode(stream, GetMaxBytesForEncoding(sizeof(ushort)));
            }

            public static int ReadInt32(Stream stream)
            {
                return (int)ReadUInt32(stream);
            }

            public static uint ReadUInt32(Stream stream)
            {
                return (uint)Decode(stream, GetMaxBytesForEncoding(sizeof(uint)));
            }

            public static long ReadInt64(Stream stream)
            {
                return (long)ReadUInt64(stream);
            }

            public static ulong ReadUInt64(Stream stream)
            {
                return Decode(stream, GetMaxBytesForEncoding(sizeof(ulong)));
            }

            public static unsafe long ReadInt16(byte* buffer, long bufferLength, out short value)
            {
                ulong readValue;
                var bytesRead = DecodeFromBuffer(buffer, bufferLength, Int32MaxBytes, out readValue);

                value = (short)readValue;
                return bytesRead;
            }

            public static unsafe long ReadUInt16(byte* buffer, long bufferLength, out ushort value)
            {
                ulong readValue;
                var bytesRead = DecodeFromBuffer(buffer, bufferLength, Int32MaxBytes, out readValue);

                value = (ushort)readValue;
                return bytesRead;
            }

            public static unsafe long ReadInt32(byte* buffer, long bufferLength, out int value)
            {
                ulong readValue;
                var bytesRead = DecodeFromBuffer(buffer, bufferLength, Int32MaxBytes, out readValue);

                value = (int)readValue;
                return bytesRead;
            }

            public static unsafe long ReadUInt32(byte* buffer, long bufferLength, out uint value)
            {
                ulong readValue;
                var bytesRead = DecodeFromBuffer(buffer, bufferLength, Int32MaxBytes, out readValue);

                value = (uint)readValue;
                return bytesRead;
            }

            public static unsafe long ReadInt64(byte* buffer, long bufferLength, out long value)
            {
                ulong readValue;
                var bytesRead = DecodeFromBuffer(buffer, bufferLength, Int64MaxBytes, out readValue);

                value = (long)readValue;
                return bytesRead;
            }

            public static unsafe long ReadUInt64(byte* buffer, long bufferLength, out ulong value)
            {
                return DecodeFromBuffer(buffer, bufferLength, Int64MaxBytes, out value);
            }

            private static unsafe long DecodeFromBuffer(byte* buffer, long bufferLength, int maxBytesInValueType, out ulong value)
            {
                var currentShift = 0;
                uint byteIndex = 0;
                ulong finalValue = 0;

                while (byteIndex <= maxBytesInValueType)
                {
                    if (byteIndex >= bufferLength)
                    {
                        throw new EndOfStreamException("Did not finish reading variable encoded buffer before hitting the end of the buffer");
                    }

                    var nextByte = buffer[byteIndex];
                    byteIndex++;

                    finalValue |= ((ulong)(nextByte & 0x7F) << currentShift);
                    currentShift += 7;

                    if ((nextByte & 0x80) == 0)
                    {
                        break;
                    }
                }

                value = finalValue;
                return byteIndex;
            }

            private static ulong Decode(Stream stream, int maxBytes)
            {
                var currentShift = 0;
                var bytesRead = 0;
                ulong finalValue = 0;

                while (bytesRead <= maxBytes)
                {
                    var nextByte = stream.ReadByte();
                    bytesRead++;

                    finalValue |= ((ulong)(nextByte & 0x7F) << currentShift);
                    currentShift += 7;

                    if ((nextByte & 0x80) == 0)
                    {
                        break;
                    }
                }

                return finalValue;
            }
        }
    }
}
