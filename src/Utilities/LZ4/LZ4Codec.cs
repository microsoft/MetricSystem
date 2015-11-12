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

namespace MetricSystem.Utilities.LZ4
{
    using System;
    using System.IO;
    using System.IO.Compression;

    using Microsoft.IO;

    /// <summary>
    /// Provides an allocator for LZ4 streams to compress and decompress data.
    /// </summary>
    public sealed unsafe class LZ4Codec
    {
        /// <summary>
        /// Determines the codec provider for LZ4 encoding.
        /// </summary>
        public enum CodecProvider
        {
            /// <summary>
            /// LZ4Net codec provider for backwards compatibility.
            /// </summary>
            LZ4Net = 0,

            /// <summary>
            /// LZ4 codec provider (directly implemented from native LZ4)
            /// XXX: Currently not supported.
            /// </summary>
            LZ4,

            /// <summary>
            /// Default provider to use (LZ4)
            /// XXX: Currently not supported.
            /// </summary>
            Default = LZ4
        }

        public const bool DefaultHighCompression = false;
        public const int DefaultBlockSize = 1 << 20;
        private readonly RecyclableMemoryStreamManager memoryStreamManager;
        private readonly ILZ4CodecProvider provider;

        /// <summary>
        /// Instantiate a new LZ4 codec handler.
        /// </summary>
        /// <param name="blockSize">Block size to compress against.</param>
        /// <param name="highCompression">Whether to use high compression mode.</param>
        /// <param name="memoryStreamManager">RecyclableMemoryStreamManager to allocate buffers from.</param>
        /// <param name="codecProvider">Which codec provider to use.</param>
        public LZ4Codec(int blockSize, bool highCompression, RecyclableMemoryStreamManager memoryStreamManager,
                        CodecProvider codecProvider)
        {
            if (blockSize < 1 << 12 || blockSize > 1 << 28)
            {
                throw new ArgumentOutOfRangeException(nameof(blockSize),
                                                      "Block size must be between 4KB and 256MB inclusive.");
            }
            if (memoryStreamManager == null)
            {
                throw new ArgumentNullException(nameof(memoryStreamManager));
            }
            switch (codecProvider)
            {
            case CodecProvider.LZ4Net:
                this.provider = new LZ4NetCodecProvider();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(codecProvider));
            }

            this.BlockSize = blockSize;
            this.HighCompression = highCompression;
            this.Codec = codecProvider;
            this.memoryStreamManager = memoryStreamManager;
        }

        /// <summary>
        /// Instantiate a new LZ4 codec handler using the default codec provider.
        /// </summary>
        /// <param name="blockSize">Block size to compress against.</param>
        /// <param name="highCompression">Whether to use high compression mode.</param>
        /// <param name="memoryStreamManager">RecyclableMemoryStreamManager to allocate buffers from.</param>
        public LZ4Codec(int blockSize, bool highCompression, RecyclableMemoryStreamManager memoryStreamManager)
            : this(blockSize, highCompression, memoryStreamManager, CodecProvider.Default) { }

        /// <summary>
        /// Instantiate a new LZ4 codec handler using the default settings for block size and high compression.
        /// </summary>
        /// <param name="memoryStreamManager">RecyclableMemoryStreamManager to allocate buffers from.</param>
        public LZ4Codec(RecyclableMemoryStreamManager memoryStreamManager, CodecProvider codecProvider)
            : this(DefaultBlockSize, DefaultHighCompression, memoryStreamManager, codecProvider) { }

        /// <summary>
        /// Instantiate a new LZ4 codec handler using the default settings for block size, high compression, and codec provider.
        /// </summary>
        /// <param name="memoryStreamManager">RecyclableMemoryStreamManager to allocate buffers from.</param>
        public LZ4Codec(RecyclableMemoryStreamManager memoryStreamManager)
            : this(DefaultBlockSize, DefaultHighCompression, memoryStreamManager, CodecProvider.Default) { }

        /// <summary>
        /// Retrieve a new stream for compression/decompression.
        /// </summary>
        /// <param name="innerStream">Inner source stream to read from or write to.</param>
        /// <param name="compressionMode">Whether to compress or decompress.</param>
        /// <param name="leaveInnerStreamOpen">If true leaves the inner stream open when the LZ4 stream is disposed.</param>
        /// <returns>A stream wrapping the inner stream to read from or write to.</returns>
        public Stream GetStream(Stream innerStream, CompressionMode compressionMode, bool leaveInnerStreamOpen)
        {
            switch (this.Codec)
            {
            case CodecProvider.LZ4Net:
                return new LZ4NetStream(innerStream, compressionMode, leaveInnerStreamOpen, this);
            default:
                throw new NotSupportedException();
            }
        }

        /// <summary>
        /// Retrieve a new stream for compression/decompression.
        /// </summary>
        /// <param name="innerStream">Inner source stream to read from or write to.</param>
        /// <param name="compressionMode">Whether to compress or decompress.</param>
        /// <returns>A stream wrapping the inner stream to read from or write to.</returns>
        public Stream GetStream(Stream innerStream, CompressionMode compressionMode)
        {
            return this.GetStream(innerStream, compressionMode, false);
        }

        /// <summary>
        /// Size of individual compressed blocks in bytes.
        /// </summary>
        public int BlockSize { get; }

        /// <summary>
        /// Whether high (more aggressive/CPU intensive) compression is used.
        /// </summary>
        public bool HighCompression { get; }

        public CodecProvider Codec { get; }

        public static long GetMaximumOutputSize(long inputBytes)
        {
            return inputBytes + (inputBytes / 255) + 16;
        }

        internal MemoryStream AllocateStream(int streamLength)
        {
            return this.memoryStreamManager.GetStream("MetricSystem/Utilities/LZ4", streamLength, true);
        }

        internal int Compress(byte[] inputBuffer, int inputLength, byte[] outputBuffer, int outputLength)
        {
            fixed (byte* input = inputBuffer, output = outputBuffer)
            {
                return this.provider.Compress(input, inputLength, output, outputLength, this.HighCompression);
            }
        }

        internal int Decompress(byte[] inputBuffer, int inputLength, byte[] outputBuffer, int outputLength)
        {
            fixed (byte* input = inputBuffer, output = outputBuffer)
            {
                return this.provider.Decompress(input, inputLength, output, outputLength);
            }
        }
    }
}