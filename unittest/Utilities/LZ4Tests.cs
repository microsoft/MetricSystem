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

namespace MetricSystem.Utilities.UnitTests
{
    using System;
    using System.IO;
    using System.IO.Compression;
    using System.Text;

    using MetricSystem.Utilities.LZ4;

    using Microsoft.IO;

    using NUnit.Framework;

    [TestFixture]
    public sealed class LZ4Tests
    {
        [Test]
        public void LZ4IsDefaultCodecProvider()
        {
            Assert.AreEqual(LZ4Codec.CodecProvider.LZ4, LZ4Codec.CodecProvider.Default);
        }

        [Test]
        public void CannotCreateInvalidCodec()
        {
            Assert.DoesNotThrow(() => new LZ4Codec(new RecyclableMemoryStreamManager(), LZ4Codec.CodecProvider.LZ4Net));
            Assert.DoesNotThrow(
                                () =>
                                new LZ4Codec(LZ4Codec.DefaultBlockSize, LZ4Codec.DefaultHighCompression,
                                             new RecyclableMemoryStreamManager(), LZ4Codec.CodecProvider.LZ4Net));

            // NOTE: LZ4 currently not supported. :)
            Assert.Throws<ArgumentOutOfRangeException>(
                                                       () =>
                                                       new LZ4Codec(new RecyclableMemoryStreamManager(),
                                                                    LZ4Codec.CodecProvider.Default));
            Assert.Throws<ArgumentOutOfRangeException>(
                                                       () =>
                                                       new LZ4Codec(LZ4Codec.DefaultBlockSize,
                                                                    LZ4Codec.DefaultHighCompression,
                                                                    new RecyclableMemoryStreamManager(),
                                                                    LZ4Codec.CodecProvider.Default));

            Assert.Throws<ArgumentOutOfRangeException>(
                                                       () =>
                                                       new LZ4Codec(new RecyclableMemoryStreamManager(),
                                                                    LZ4Codec.CodecProvider.Default + 1));
            Assert.Throws<ArgumentOutOfRangeException>(
                                                       () =>
                                                       new LZ4Codec(LZ4Codec.DefaultBlockSize,
                                                                    LZ4Codec.DefaultHighCompression,
                                                                    new RecyclableMemoryStreamManager(),
                                                                    LZ4Codec.CodecProvider.Default + 1));
        }

        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        [Test]
        public void CanCompressAndDecompressData(bool highCompression, bool randomData)
        {
            var codec = new LZ4Codec(LZ4Codec.DefaultBlockSize, highCompression, new RecyclableMemoryStreamManager(),
                                     LZ4Codec.CodecProvider.LZ4Net);
            for (var bufferLength = 10; bufferLength < 10000000; bufferLength *= 10)
            {
                var sourceBuffer = randomData
                                       ? GenerateRandomDataBuffer(bufferLength)
                                       : GenerateNonrandomDataBuffer(bufferLength);
                using (var compressedStream = new MemoryStream())
                {
                    using (var compressionStream = codec.GetStream(compressedStream, CompressionMode.Compress, true))
                    {
                        compressionStream.Write(sourceBuffer, 0, sourceBuffer.Length);
                    }

                    compressedStream.Position = 0;
                    var destinationBuffer = new byte[sourceBuffer.Length];
                    using (var decompressionStream = codec.GetStream(compressedStream, CompressionMode.Decompress, true))
                    {
                        decompressionStream.Read(destinationBuffer, 0, destinationBuffer.Length);
                    }

                    for (var i = 0; i < bufferLength; ++i)
                    {
                        Assert.AreEqual(sourceBuffer[i], destinationBuffer[i]);
                    }
                }
            }
        }

        private static byte[] GenerateRandomDataBuffer(int bufferLength)
        {
            var buffer = new byte[bufferLength];
            var rng = new Random();
            rng.NextBytes(buffer);
            return buffer;
        }

        private static readonly byte[] NonrandomData = Encoding.UTF8.GetBytes("abcdefghijklmnopqrstuvwxyz0123456789");
        private static byte[] GenerateNonrandomDataBuffer(int bufferLength)
        {
            var buffer = new byte[bufferLength];
            int offset = 0;
            while (offset < buffer.Length)
            {
                var copyBytes = Math.Min(NonrandomData.Length, buffer.Length - offset);
                Buffer.BlockCopy(NonrandomData, 0, buffer, offset, copyBytes);
                offset += copyBytes;
            }

            return buffer;
        }
    }
}