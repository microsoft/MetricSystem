// ---------------------------------------------------------------------
// <copyright file="PersistedDataWriter.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.IO.Compression;

    using Microsoft.IO;
    using MetricSystem.Utilities;

    using VariableLengthEncoding = MetricSystem.Utilities.ByteConverter.VariableLengthEncoding;

    /// <summary>
    /// Writer for serializing data to a stream.
    /// </summary>
    internal sealed class PersistedDataWriter : IDisposable
    {
        public delegate IEnumerable<KeyValuePair<Key, IInternalData>> SerializableDataProvider();

        private readonly DimensionSet dimensionSet;
        private readonly RecyclableMemoryStreamManager memoryStreamManager;
        private long blockLengthOffset;
        private long blockStartOffset;

        private readonly Stream sourceStream;

        private WriterStream sourceStreamWriter;

        public PersistedDataWriter(Stream stream, DimensionSet dimensionSet, RecyclableMemoryStreamManager memoryStreamManager)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }
            if (dimensionSet == null)
            {
                throw new ArgumentNullException("dimensionSet");
            }

            if (!stream.CanSeek)
            {
                throw new NotSupportedException("Stream must be able to seek.");
            }

            this.sourceStream = stream;
            this.dimensionSet = dimensionSet;
            this.memoryStreamManager = memoryStreamManager;
            this.sourceStreamWriter = new WriterStream(this.sourceStream, this.memoryStreamManager);

            this.WriteHeader();
        }

        public void Dispose()
        {
            if (this.sourceStreamWriter != null)
            {
                this.sourceStreamWriter.Dispose();
                this.sourceStreamWriter = null;
            }
        }

        public void WriteData<TInternal>(string name, DateTime start, DateTime end, uint dataCount,
                                         IEnumerable<PersistedDataSource> sources, KeyedDataStore<TInternal> data)
            where TInternal : class, IInternalData, new()
        {
            var header = new PersistedDataHeader(name, start, end,
                                                 PersistedDataProtocol.GetPersistedTypeCodeFromType(typeof(TInternal)),
                                                 sources, this.dimensionSet, dataCount);

            this.WriteDataWithLengthAndCRC32(ms =>
                                             {
                                                 header.Write(new BufferWriter(ms));
                                             }, header.SerializedSize, true);

            // The raw data does not compress particularly well. There is some overlap in the keys, but particularly
            // for histograms they come pre-compressed. We also use VLE heavily in KeyedDataStore.Serialize which
            // makes for pretty compact data.
            this.WriteDataWithLengthAndCRC32(data.Serialize, data.SerializedSize, false);

            // We can now determine what our block length really is and back-fill our data.
            var currentPosition = this.sourceStream.Position;
            var blockLength = this.sourceStream.Position - this.blockStartOffset;
            this.sourceStream.Position = this.blockLengthOffset;
            this.sourceStreamWriter.WriteUInt64((ulong)blockLength);
            this.sourceStream.Position = currentPosition;
        }

        private void WriteHeader()
        {
            this.sourceStreamWriter.WriteUInt16(PersistedDataProtocol.ProtocolVersion);
            this.blockLengthOffset = this.sourceStream.Position;
            // Write a dummy offset to start.
            this.sourceStreamWriter.WriteUInt64(0);
            this.blockStartOffset = this.sourceStream.Position;
        }

        // Write a block of (optionally compressed) data with a length prefix and the CRC32 of the uncompressed contents.
        // The length prefix is necessary due to peculiar behavior of .NET's DeflateStream (and GZipStream) which chew through
        // an entire stream instead of reading only the compressed portion and stopping.
        private void WriteDataWithLengthAndCRC32(Action<MemoryStream> writeAction, long suggestedLength, bool shouldCompress)
        {
            using (var ms = this.memoryStreamManager.GetStream("PersistedDataWriter", (int)suggestedLength))
            {
                var startPosition = this.sourceStream.Position;
                // Record a placeholder for the total length of the written data (including optional uncompressed length and CRC32)
                this.sourceStreamWriter.WriteUInt64(0);
                if (shouldCompress)
                {
                    // For compressed data we record both the overall data block length and the uncompressed data length.
                    // Recording the uncompressed data length affords efficient memory allocation when data is read back
                    // from storage.
                    this.sourceStreamWriter.WriteUInt64(0);
                }

                writeAction(ms);

                var uncompressedLength = ms.Position;
                var crc32 = CRC32.Compute(ms.GetBuffer(), 0, uncompressedLength);
                this.sourceStreamWriter.WriteUInt32(crc32);

                ms.Position = 0;
                if (shouldCompress)
                {
                    using (var compressionStream = new DeflateStream(this.sourceStream, CompressionLevel.Fastest, true))
                    {
                        ms.CopyTo(compressionStream);
                        compressionStream.Flush();
                    }
                }
                else
                {
                    ms.CopyTo(this.sourceStream);
                }

                var currentPosition = this.sourceStream.Position;
                var length = currentPosition - startPosition - sizeof(long);
                if ((length & PersistedDataProtocol.CompressionFlag) != 0)
                {
                    // This is wildly unlikely at the time of authoring. :)
                    throw new PersistedDataException("Written data length exceeds allowed alue.");
                }

                this.sourceStream.Position = startPosition;
                if (shouldCompress)
                {
                    this.sourceStreamWriter.WriteUInt64((ulong)(length | PersistedDataProtocol.CompressionFlag));
                    this.sourceStreamWriter.WriteUInt64((ulong)uncompressedLength);
                }
                else
                {
                    this.sourceStreamWriter.WriteUInt64((ulong)length);
                }
                this.sourceStream.Position = currentPosition;
            }
        }
    }
}
