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

    using Bond;
    using Bond.IO;
    using Bond.Protocols;

    using Microsoft.IO;

    /// <summary>
    /// Reader which pulls bytes directly from an underlying string with GC-optimized intermediate buffering 
    /// </summary>
    public class ReaderStream : IInputStream, ICloneable<ReaderStream>, IDisposable
    {
        /// <summary>
        /// GC-optimized intermediate buffer 
        /// </summary>
        private readonly MemoryStream bufferStream;

        /// <summary>
        /// Optional memory stream manager which pools buffer streams efficiently
        /// </summary>
        private readonly RecyclableMemoryStreamManager memStreamManager;

        /// <summary>
        /// True if this object is responsible for also disposing the underlying internal stream
        /// </summary>
        private readonly bool needToDisposeInnerStream;

        /// <summary>
        /// Internal stream holding the actual contents
        /// </summary>
        private Stream innerStream;

        public ReaderStream(Stream innerStream)
            : this(innerStream, null, false) { }

        public ReaderStream(Stream innerStream, RecyclableMemoryStreamManager memStreamManager, bool needToDisposeStream)
        {
            if (innerStream == null || !innerStream.CanRead)
            {
                throw new ArgumentException("input stream must support read operations");
            }

            this.innerStream = innerStream;
            this.needToDisposeInnerStream = needToDisposeStream;
            this.memStreamManager = memStreamManager;

            this.bufferStream = this.memStreamManager != null
                                    ? this.memStreamManager.GetStream("BondReaderStream", sizeof(ulong))
                                    : new MemoryStream(sizeof(ulong));
        }

        /// <summary>
        /// Constructor which creates a reader out of an underlying buffer
        /// </summary>
        public ReaderStream(byte[] buffer, int bufferOffset, long bufferLength,
                            RecyclableMemoryStreamManager memoryStreamManager = null) :
                                this(
                                new ArraySegmentStream(new ArraySegment<byte>(buffer, bufferOffset, (int)bufferLength)),
                                memoryStreamManager, false) { }

        public ReaderStream Clone()
        {
            var newStream = this.CloneStream();
            newStream.Position = this.Position;

            // BUG! Do NOT pass in the original memstream manager since this readerstream will not be disposed
            // (Core bond runtime does not dispose cloned streams. Therefore we know the buffered stream will be leaked)
            return new ReaderStream(newStream, null, true);
        }

        public void Dispose()
        {
            if (this.needToDisposeInnerStream && this.innerStream != null)
            {
                this.innerStream.Dispose();
                this.innerStream = null;
            }

            if (this.bufferStream != null)
            {
                this.bufferStream.Dispose();
            }
        }

        public long Position
        {
            get { return this.innerStream.Position; }
            set { this.innerStream.Position = value; }
        }

        public long Length
        {
            get { return this.innerStream.Length; }
        }

        public void SkipBytes(int count)
        {
            this.Position += count;
        }

        public ArraySegment<byte> ReadBytes(int count)
        {
            if (this.bufferStream.Capacity < count)
            {
                this.bufferStream.Capacity = count;
            }

            var buffer = this.bufferStream.GetBuffer();
            this.innerStream.Read(buffer, 0, count);
            return new ArraySegment<byte>(buffer, 0, count);
        }

        public double ReadDouble()
        {
            var data = this.ReadBytes(sizeof(double));
            return BitConverter.ToDouble(data.Array, data.Offset);
        }

        public float ReadFloat()
        {
            var data = this.ReadBytes(sizeof(float));
            return BitConverter.ToSingle(data.Array, data.Offset);
        }

        public string ReadString(Encoding encoding, int size)
        {
            var bytes = this.ReadBytes(size);
            return encoding.GetString(bytes.Array, bytes.Offset, bytes.Count);
        }

        public ushort ReadUInt16()
        {
            var data = this.ReadBytes(sizeof(ushort));
            return BitConverter.ToUInt16(data.Array, data.Offset);
        }

        public uint ReadUInt32()
        {
            var data = this.ReadBytes(sizeof(uint));
            return BitConverter.ToUInt32(data.Array, data.Offset);
        }

        public ulong ReadUInt64()
        {
            var data = this.ReadBytes(sizeof(ulong));
            return BitConverter.ToUInt64(data.Array, data.Offset);
        }

        public byte ReadUInt8()
        {
            var b = this.innerStream.ReadByte();
            if (b == -1)
            {
                throw new EndOfStreamException();
            }

            return (byte)b;
        }

        public ushort ReadVarUInt16()
        {
            return ByteConverter.VariableLengthEncoding.ReadUInt16(this.innerStream);
        }

        public uint ReadVarUInt32()
        {
            return ByteConverter.VariableLengthEncoding.ReadUInt32(this.innerStream);
        }

        public ulong ReadVarUInt64()
        {
            return ByteConverter.VariableLengthEncoding.ReadUInt64(this.innerStream);
        }

        /// <summary>
        /// Static ctor that creates a reader stream from a memory stream whose buffer is accessible and starts at offset 0.
        /// This is the default use-case in MetricSystem programming
        /// </summary>
        public static ReaderStream FromMemoryStreamBuffer(MemoryStream memoryStream,
                                                          RecyclableMemoryStreamManager memoryStreamManager)
        {
            return new ReaderStream(memoryStream.GetBuffer(), 0, memoryStream.Length, memoryStreamManager);
        }

        public CompactBinaryReader<ReaderStream> CreateCompactBinaryReader()
        {
            return new CompactBinaryReader<ReaderStream>(this);
        }

        public SimpleJsonReader CreateSimpleJsonReader()
        {
            return new SimpleJsonReader(this.innerStream);
        }

        public SimpleBinaryReader<ReaderStream> CreateSimpleBinaryReader()
        {
            return new SimpleBinaryReader<ReaderStream>(this);
        }

        public IBonded<T> CreateBondedCompactBinaryReader<T>()
        {
            return new Bonded<T, CompactBinaryReader<ReaderStream>>(this.CreateCompactBinaryReader());
        }

        public IBonded<T> CreateBondedSimpleBinaryReader<T>()
        {
            return new Bonded<T, SimpleBinaryReader<ReaderStream>>(this.CreateSimpleBinaryReader());
        }

        private Stream CloneStream()
        {
            var arraySegmentStream = this.innerStream as ArraySegmentStream;
            if (arraySegmentStream != null)
            {
                return new ArraySegmentStream(
                    new ArraySegment<byte>(arraySegmentStream.OriginalSegment.Array,
                                           arraySegmentStream.OriginalSegment.Offset,
                                           arraySegmentStream.OriginalSegment.Count -
                                           arraySegmentStream.OriginalSegment.Offset)
                    );
            }

            var memoryStream = this.innerStream as MemoryStream;
            if (memoryStream != null)
            {
                var newArray = memoryStream.ToArray();
                return new ArraySegmentStream(
                    new ArraySegment<byte>(newArray, 0,
                                           newArray.Length));
            }

            var fileStream = this.innerStream as FileStream;
            if (fileStream != null)
            {
                //BUG BUG BUG - this stream will not be disposed :(. Need to fix core bond
                return new FileStream(fileStream.Name, FileMode.Open, FileAccess.Read, FileShare.Read);
            }

            var clonable = this.innerStream as ICloneable;
            if (clonable != null)
            {
                return (Stream)clonable.Clone();
            }

            // some other type of stream - just copy all the bytes. 
            var newBuffer = new byte[this.innerStream.Length];
            var oldPosition = this.Position;
            this.innerStream.Position = 0;
            this.innerStream.Read(newBuffer, 0, newBuffer.Length);
            this.innerStream.Position = oldPosition;

            return new ArraySegmentStream(
                new ArraySegment<byte>(newBuffer, 0, newBuffer.Length));
        }
    }
}
