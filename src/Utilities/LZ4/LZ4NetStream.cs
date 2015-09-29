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

// Copyright (c) 2013, Milosz Krajewski
// All rights reserved.

// Redistribution and use in source and binary forms, with or without modification, are permitted provided 
// that the following conditions are met:

// * Redistributions of source code must retain the above copyright notice, this list of conditions 
//   and the following disclaimer.

// * Redistributions in binary form must reproduce the above copyright notice, this list of conditions 
//   and the following disclaimer in the documentation and/or other materials provided with the distribution.

// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
// WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR 
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT 
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN 
// IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

namespace MetricSystem.Utilities.LZ4
{
    using System;
    using System.IO;
    using System.IO.Compression;

    public sealed class LZ4NetStream : Stream
    {
        private readonly LZ4Codec codec;
        private readonly CompressionMode compressionMode;
        private readonly Stream innerStream;
        private readonly bool leaveInnerStreamOpen;
        private int bufferLength;
        private int bufferOffset;
        private MemoryStream bufferStream;

        internal LZ4NetStream(Stream innerStream, CompressionMode compressionMode, bool leaveInnerStreamOpen,
                              LZ4Codec codec)
        {
            if (innerStream == null)
            {
                throw new ArgumentNullException(nameof(innerStream));
            }
            switch (compressionMode)
            {
            case CompressionMode.Compress:
            case CompressionMode.Decompress:
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(compressionMode));
            }
            if (codec == null)
            {
                throw new ArgumentNullException(nameof(codec));
            }

            this.innerStream = innerStream;
            this.compressionMode = compressionMode;
            this.leaveInnerStreamOpen = leaveInnerStreamOpen;
            this.codec = codec;
        }

        private byte[] buffer => this.bufferStream?.GetBuffer();
        public override bool CanRead => this.compressionMode == CompressionMode.Decompress;

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite => this.compressionMode == CompressionMode.Compress;

        public override long Length
        {
            get { throw new NotSupportedException("Cannot get compression stream length."); }
        }

        public override long Position
        {
            get { throw new NotSupportedException("Cannot get compression stream position."); }
            set { throw new NotSupportedException("Cannot set compression stream position."); }
        }

        /// <summary>Tries to read variable length int.</summary>
        /// <param name="result">The result.</param>
        /// <returns><c>true</c> if integer has been read, <c>false</c> if end of stream has been
        /// encountered. If end of stream has been encoutered in the middle of value 
        /// <see cref="EndOfStreamException"/> is thrown.</returns>
        private bool TryReadVarInt(out ulong result)
        {
            var count = 0;
            result = 0;

            while (true)
            {
                int readByte;
                if ((readByte = this.innerStream.ReadByte()) == -1)
                {
                    if (count == 0)
                    {
                        return false;
                    }
                    throw new EndOfStreamException();
                }
                var b = (byte)readByte;
                result = result + ((ulong)(b & 0x7F) << count);
                count += 7;
                if ((b & 0x80) == 0 || count >= 64)
                {
                    break;
                }
            }

            return true;
        }

        /// <summary>Reads the variable length int. Work with assumption that value is in the stream
        /// and throws exception if it isn't. If you want to check if value is in the stream
        /// use <see cref="TryReadVarInt"/> instead.</summary>
        /// <returns></returns>
        private ulong ReadVarInt()
        {
            ulong result;
            if (!this.TryReadVarInt(out result))
            {
                throw new EndOfStreamException();
            }
            return result;
        }

        /// <summary>Reads the block of bytes. 
        /// Contrary to <see cref="Stream.Read"/> does not read partial data if possible. 
        /// If there is no data (yet) it waits.</summary>
        /// <param name="buffer">The buffer.</param>
        /// <param name="offset">The offset.</param>
        /// <param name="length">The length.</param>
        /// <returns>Number of bytes read.</returns>
        private int ReadBlock(byte[] outputBuffer, int offset, int length)
        {
            var total = 0;

            while (length > 0)
            {
                var read = this.innerStream.Read(outputBuffer, offset, length);
                if (read == 0)
                {
                    break;
                }
                offset += read;
                length -= read;
                total += read;
            }

            return total;
        }

        /// <summary>Writes the variable length integer.</summary>
        /// <param name="value">The value.</param>
        private void WriteVarInt(ulong value)
        {
            while (true)
            {
                var b = (byte)(value & 0x7F);
                value >>= 7;
                this.innerStream.WriteByte((byte)(b | (value == 0 ? 0 : 0x80)));
                if (value == 0)
                {
                    break;
                }
            }
        }

        /// <summary>Flushes current chunk.</summary>
        private void FlushCurrentChunk()
        {
            if (this.bufferOffset <= 0)
            {
                return;
            }

            using (var compressedStream = this.codec.AllocateStream(this.bufferOffset))
            {
                var compressed = compressedStream.GetBuffer();
                var compressedLength = this.codec.Compress(this.buffer, this.bufferOffset, compressed, this.bufferOffset);

                bool isCompressed = true;
                var flags = this.codec.HighCompression ? ChunkFlags.HighCompression : ChunkFlags.None;

                if (compressedLength <= 0 || compressedLength >= this.bufferOffset)
                {
                    // uncompressible block
                    compressed = this.buffer;
                    compressedLength = this.bufferOffset;
                    isCompressed = false;
                }

                if (isCompressed)
                {
                    flags |= ChunkFlags.Compressed;
                }

                this.WriteVarInt((ulong)flags);
                this.WriteVarInt((ulong)this.bufferOffset);
                if (isCompressed)
                {
                    this.WriteVarInt((ulong)compressedLength);
                }

                this.innerStream.Write(compressed, 0, compressedLength);

                this.bufferOffset = 0;
            }
        }

        /// <summary>Reads the next chunk from stream.</summary>
        /// <returns><c>true</c> if next has been read, or <c>false</c> if it is legitimate end of file.
        /// Throws <see cref="EndOfStreamException"/> if end of stream was unexpected.</returns>
        private bool AcquireNextChunk()
        {
            do
            {
                ulong varint;
                if (!this.TryReadVarInt(out varint))
                {
                    return false;
                }
                var flags = (ChunkFlags)varint;
                var isCompressed = (flags & ChunkFlags.Compressed) != 0;

                var originalLength = (int)this.ReadVarInt();
                var compressedLength = isCompressed ? (int)this.ReadVarInt() : originalLength;
                if (compressedLength > originalLength)
                {
                    throw new EndOfStreamException(); // corrupted
                }

                var compressedStream = this.codec.AllocateStream(compressedLength);
                var compressed = compressedStream.GetBuffer();
                var chunk = this.ReadBlock(compressed, 0, compressedLength);

                if (chunk != compressedLength)
                {
                    throw new EndOfStreamException(); // currupted
                }

                if (!isCompressed)
                {
                    this.bufferStream?.Dispose();
                    this.bufferStream = compressedStream; // no compression on this chunk
                    this.bufferLength = compressedLength;
                }
                else
                {
                    if (this.bufferStream == null || this.bufferStream.Length < originalLength)
                    {
                        this.bufferStream?.Dispose();
                        this.bufferStream = this.codec.AllocateStream(originalLength);
                    }

                    var passes = (int)flags >> 2;
                    if (passes != 0)
                    {
                        throw new NotSupportedException("Chunks with multiple passes are not supported.");
                    }
                    this.codec.Decompress(compressed, compressedLength, this.buffer, originalLength);
                    this.bufferLength = originalLength;
                    compressedStream.Dispose();
                }

                this.bufferOffset = 0;
            } while (this.bufferLength == 0); // skip empty block (shouldn't happen but...)

            return true;
        }

        public override void Flush()
        {
            if (this.bufferOffset > 0 && this.CanWrite)
            {
                this.FlushCurrentChunk();
            }
        }

        /// <summary>Reads a byte from the stream and advances the position within the stream by one byte, or returns -1 if at the end of the stream.</summary>
        /// <returns>The unsigned byte cast to an Int32, or -1 if at the end of the stream.</returns>
        public override int ReadByte()
        {
            if (!this.CanRead)
            {
                throw new NotSupportedException("Cannot read from compression stream.");
            }

            if (this.bufferOffset >= this.bufferLength && !this.AcquireNextChunk())
            {
                return -1; // that's just end of stream
            }

            return this.buffer[this.bufferOffset++];
        }

        public override int Read(byte[] outputBuffer, int offset, int count)
        {
            if (!this.CanRead)
            {
                throw new NotSupportedException("Cannot read from compression stream.");
            }

            var total = 0;

            while (count > 0)
            {
                var chunk = Math.Min(count, this.bufferLength - this.bufferOffset);
                if (chunk > 0)
                {
                    Buffer.BlockCopy(this.buffer, this.bufferOffset, outputBuffer, offset, chunk);
                    this.bufferOffset += chunk;
                    offset += chunk;
                    count -= chunk;
                    total += chunk;
                }
                else
                {
                    if (!this.AcquireNextChunk())
                    {
                        break;
                    }
                }
            }

            return total;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException("Cannot seek within LZ4 stream.");
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException("Cannot set length of LZ4 stream.");
        }

        /// <summary>Writes a byte to the current position in the stream and advances the position within the stream by one byte.</summary>
        /// <param name="value">The byte to write to the stream.</param>
        public override void WriteByte(byte value)
        {
            if (!this.CanWrite)
            {
                throw new NotSupportedException("Cannot write to decompression stream.");
            }

            if (this.bufferStream == null)
            {
                this.bufferStream = this.codec.AllocateStream(this.codec.BlockSize);
                this.bufferLength = this.codec.BlockSize;
                this.bufferOffset = 0;
            }

            if (this.bufferOffset >= this.bufferLength)
            {
                this.FlushCurrentChunk();
            }

            this.buffer[this.bufferOffset++] = value;
        }

        public override void Write(byte[] inputBuffer, int offset, int count)
        {
            if (!this.CanWrite)
            {
                throw new NotSupportedException("Cannot write to decompression stream.");
            }

            if (this.bufferStream == null)
            {
                this.bufferStream = this.codec.AllocateStream(this.codec.BlockSize);
                this.bufferLength = this.codec.BlockSize;
                this.bufferOffset = 0;
            }

            while (count > 0)
            {
                var chunk = Math.Min(count, this.bufferLength - bufferOffset);
                if (chunk > 0)
                {
                    Buffer.BlockCopy(inputBuffer, offset, this.buffer, this.bufferOffset, chunk);
                    offset += chunk;
                    count -= chunk;
                    this.bufferOffset += chunk;
                }
                else
                {
                    this.FlushCurrentChunk();
                }
            }
        }

        /// <summary>Releases the unmanaged resources used by the <see cref="T:System.IO.Stream" /> and optionally releases the managed resources.</summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.Flush();
                if (!this.leaveInnerStreamOpen)
                {
                    this.innerStream.Dispose();
                }
                this.bufferStream?.Dispose();
            }

            base.Dispose(disposing);
        }

        [Flags]
        private enum ChunkFlags
        {
            /// <summary>None.</summary>
            None = 0x00,

            /// <summary>Set if chunk is compressed.</summary>
            Compressed = 0x01,

            /// <summary>Set if high compression has been selected (does not affect decoder, 
            /// but might be useful when rewriting)</summary>
            HighCompression = 0x02,
        }
    }
}