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
    using System;
    using System.IO;
    using System.IO.Compression;

    using Microsoft.IO;
    using MetricSystem.Utilities;
    using MetricSystem.Utilities.LZ4;

    using VariableLengthEncoding = MetricSystem.Utilities.ByteConverter.VariableLengthEncoding;

    internal sealed class PersistedDataReader : IDisposable
    {
        public delegate void DataHandler<in TInternal>(Key key, TInternal data)
            where TInternal : IInternalData, new();

        public delegate void DataHeaderHandler(string name, DateTime start, DateTime end, Type dataType);

        private readonly RecyclableMemoryStreamManager memoryStreamManager;
        private Stream sourceStream;
        private uint pendingObjects;
        private long nextHeaderOffset = 0;
        private bool usePreviousProtocol;

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="stream">Stream to read from.</param>
        /// <param name="memoryStreamManager">RecyclableMemoryStream manager to use for getting memory streams.</param>
        public PersistedDataReader(Stream stream, RecyclableMemoryStreamManager memoryStreamManager) :
            this(stream, memoryStreamManager, null) { }

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="stream">Stream to read from.</param>
        /// <param name="memoryStreamManager">RecyclableMemoryStream manager to use for getting memory streams.</param>
        /// <param name="targetDimensionSet">Target dimension set to convert keys to (may be null for no conversion).</param>
        public PersistedDataReader(Stream stream, RecyclableMemoryStreamManager memoryStreamManager,
            DimensionSet targetDimensionSet)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            if (memoryStreamManager == null)
            {
                throw new ArgumentNullException("memoryStreamManager");
            }

            if (!stream.CanSeek)
            {
                throw new NotSupportedException("Must provide a Stream which supports seek operations.");
            }

            this.sourceStream = stream;
            this.memoryStreamManager = memoryStreamManager;
            this.TargetDimensionSet = targetDimensionSet;
        }

        /// <summary>
        /// The current header (read from <see cref="ReadDataHeader"/>
        /// </summary>
        public PersistedDataHeader Header { get; private set; }

        /// <summary>
        /// Protocol version of the data.
        /// </summary>
        public ushort Version { get; private set; }

        /// <summary>
        /// True if the data was serialized using the most recent known protocol version.
        /// </summary>
        public bool IsLatestProtocol { get { return !this.usePreviousProtocol; } }

        /// <summary>
        /// The parsed start time for the current header in UTC.
        /// </summary>
        public DateTime StartTime
        {
            get
            {
                return this.Header.StartTime.UtcDateTime;
            }
        }

        /// <summary>
        /// The parsed end time for the current header in UTC.
        /// </summary>
        public DateTime EndTime
        {
            get
            {
                return this.Header.EndTime.UtcDateTime;
            }
        }

        /// <summary>
        /// The type expected for data as read from the current data header.
        /// </summary>
        public Type DataType
        {
            get { return PersistedDataProtocol.GetTypeFromPersistedTypeCode(this.Header.DataType); }
        }

        /// <summary>
        /// The current dimension set (read from <see cref="ReadDataHeader"/>)
        /// </summary>
        public DimensionSet DimensionSet
        {
            get { return this.Header.DimensionSet; }
        }

        /// <summary>
        /// Dimension set to use when performing key conversion on read data.
        /// </summary>
        public DimensionSet TargetDimensionSet { get; set; }

        public bool ReadDataHeader()
        {
            Events.Write.BeginReadPersistedDataHeader();

            if (this.pendingObjects != 0)
            {
                throw new PersistedDataException("Attempted to read data header without reading pending data objects");
            }


            this.usePreviousProtocol = false;
            if (this.nextHeaderOffset > 0)
            {
                if (this.nextHeaderOffset >= this.sourceStream.Length)
                {
                    return false;
                }

                this.sourceStream.Position = this.nextHeaderOffset;
            }

            using (var readerStream = new ReaderStream(this.sourceStream, this.memoryStreamManager, false))
            {
                try
                {
                    var version = readerStream.ReadUInt16();
                    if (version != PersistedDataProtocol.ProtocolVersion)
                    {
                        if (version == PersistedDataProtocol.PreviousProtocolVersion)
                        {
                            this.usePreviousProtocol = true;
                        }
                        else
                        {
                            throw new PersistedDataException("Attempted to read protocol data of unsupported version.");
                        }
                    }
                    this.Version = version;
                }
                catch (EndOfStreamException)
                {
                    // This is dumb but we don't have a better way to handle this condition. It's okay if we're at the end
                    // of the stream when reading the version header, it just means we don't have more data in the payload.
                    return false;
                }
                try
                {
                    var blockLength = readerStream.ReadUInt64();
                    this.nextHeaderOffset = this.sourceStream.Position + (long)blockLength;
                    if (this.usePreviousProtocol)
                    {
                        this.Header = this.LoadLegacyHeader();
                    }
                    else
                    {
                        this.Header = this.LoadHeader();
                    }

                    if (this.TargetDimensionSet == null)
                    {
                        this.TargetDimensionSet = this.DimensionSet;
                    }
                    this.pendingObjects = this.Header.DataCount;

                    Events.Write.EndReadPersistedDataHeader();
                    return true;
                }
                catch (Exception ex)
                {
                    if (ex is EndOfStreamException || ex is InvalidDataException)
                    {
                        throw new PersistedDataException("Stream data may be truncated", ex);
                    }

                    throw;
                }
            }
        }

        private void CheckedRead(byte[] destination, long expectedLength)
        {
            var read = this.sourceStream.Read(destination, 0, (int)expectedLength);
            if (read != expectedLength)
            {
                throw new PersistedDataException(string.Format("Read {0} bytes, expected {1}", read, expectedLength));
            }
        }

        private static void LegacyCheckedRead(Stream stream, byte[] destination, long expectedLength)
        {
            var read = stream.Read(destination, 0, (int)expectedLength);
            if (read != expectedLength)
            {
                throw new PersistedDataException(string.Format("Read {0} bytes, expected {1}", read, expectedLength));
            }
        }

        private T LoadAndValidateData<T>(Func<MemoryStream, byte[], long, T> readAction)
        {
            MemoryStream memoryStream = null;
            try
            {
                var startPosition = this.sourceStream.Position;

                // Read header values for the next block
                var valueData = new byte[sizeof(long)];
                long length;
                long dataLength;
                bool compressed;
                uint crc32;
                if (this.usePreviousProtocol)
                {
                    // legacy data is a four byte int, not 8.
                    this.CheckedRead(valueData, sizeof(int));
                    dataLength = length = BitConverter.ToInt32(valueData, 0);
                    // for the legacy data we'll read we know it's never compressed.
                    compressed = false;
                    this.CheckedRead(valueData, sizeof(int));
                    crc32 = BitConverter.ToUInt32(valueData, 0);
                }
                else
                {
                    this.CheckedRead(valueData, sizeof(long));
                    length = BitConverter.ToInt64(valueData, 0);

                    dataLength = length - sizeof(uint);
                    PersistedDataProtocol.CompressionType compressionType;
                    length = PersistedDataProtocol.DeserializeBufferLengthValue(length, out compressed,
                                                                                out compressionType);
                    if (compressed)
                    {
                        this.CheckedRead(valueData, sizeof(long));
                        dataLength = BitConverter.ToInt64(valueData, 0);
                    }
                    this.CheckedRead(valueData, sizeof(uint));
                    crc32 = BitConverter.ToUInt32(valueData, 0);
                }

                memoryStream = this.memoryStreamManager.GetStream("PersistedDataReader", (int)dataLength, true);
                // Because we just use the underlying buffer the length won't be set by conventional methods, so we must do so
                // manually.
                memoryStream.SetLength(dataLength);
                var buffer = memoryStream.GetBuffer();
                if (compressed)
                {
                    using (var compressionStream = new DeflateStream(this.sourceStream, CompressionMode.Decompress, true))
                    {
                        compressionStream.Read(buffer, 0, (int)dataLength);
                    }
                    // DeflateStream (and GZipStream) have this amazingly offensive behavior where they read through the whole
                    // stream instead of stopping at the end of compressed data. Soooo let's deal with that and set the stream
                    // position to what you'd expect.
                    this.sourceStream.Position = startPosition + length + sizeof(long);
                }
                else
                {
                    this.CheckedRead(buffer, dataLength);
                }

                var dataCRC = CRC32.Compute(buffer, dataLength);

                if (crc32 != dataCRC)
                {
                    throw new PersistedDataException(string.Format("CRC failed for data, expected {0} was {1}",
                                                                   crc32, dataCRC));
                }

                return readAction(memoryStream, buffer, dataLength);
            }
            catch (Exception ex)
            {
                if (memoryStream != null)
                {
                    memoryStream.Dispose();
                }
                if (ex is EndOfStreamException || ex is InvalidDataException)
                {
                    throw new PersistedDataException("Stream data may be truncated", ex);
                }

                throw;
            }
        }

        private unsafe PersistedDataHeader LoadLegacyHeader()
        {
            MemoryStream memoryStream = null;
            try
            {
                // legacy data used a variant of LZ4Net.
                var codec = new LZ4Codec(this.memoryStreamManager, LZ4Codec.CodecProvider.LZ4Net);
                using (var decompressionStream = codec.GetStream(this.sourceStream, CompressionMode.Decompress, true))
                {
                    var lengthData = new byte[4];
                    LegacyCheckedRead(decompressionStream, lengthData, sizeof(int));
                    var length = BitConverter.ToUInt32(lengthData, 0);
                    var crcData = new byte[4];
                    LegacyCheckedRead(decompressionStream, crcData, sizeof(int));
                    var crc32 = BitConverter.ToUInt32(crcData, 0);

                    memoryStream = this.memoryStreamManager.GetStream("PersistedDataReader", (int)length, true);
                    memoryStream.SetLength(length);
                    var buffer = memoryStream.GetBuffer();
                    LegacyCheckedRead(decompressionStream, buffer, length);
                    var dataCRC = CRC32.Compute(buffer, length);

                    if (crc32 != dataCRC)
                    {
                        throw new PersistedDataException(string.Format("CRC failed for data, expected {0} was {1}",
                                                                       crc32, dataCRC));
                    }

                    PersistedDataHeader header;
                    fixed (byte* buf = buffer)
                    {
                        header = new PersistedDataHeader(new BufferReader(buf, length));
                    }

                    memoryStream.Dispose();
                    return header;
                }
            }
            catch (Exception ex)
            {
                memoryStream?.Dispose();
                if (ex is EndOfStreamException || ex is InvalidDataException)
                {
                    throw new PersistedDataException("Stream data may be truncated", ex);
                }

                throw;
            }
        }

        private unsafe PersistedDataHeader LoadHeader()
        {
            return this.LoadAndValidateData((ms, buffer, length) =>
                                            {
                                                // Do not do 'using' here because if an exception leaks we are
                                                // cleaned up elsewhere.
                                                PersistedDataHeader header;
                                                fixed (byte* buf = buffer)
                                                {
                                                    header = new PersistedDataHeader(new BufferReader(buf, length));
                                                }
                                                ms.Dispose();
                                                return header;
                                            });
        }

        /// <summary>
        /// Directly loads data with no transformation.
        /// </summary>
        /// <typeparam name="TInternal">Type of the internal data storage class.</typeparam>
        /// <returns>The loaded data.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public KeyedDataStore<TInternal> LoadData<TInternal>()
            where TInternal : class, IInternalData, new()
        {
            PersistedDataType dataType = this.Header.DataType;
            KeyedDataStore<TInternal> store =
                this.LoadAndValidateData((ms, buffer, length) =>
                                         {
                                             var newStore = new KeyedDataStore<TInternal>(
                                                 this.DimensionSet, this.memoryStreamManager,
                                                 ms, (int)this.pendingObjects, dataType, "new");
                                             this.pendingObjects -= (uint)newStore.Count;
                                             return newStore;
                                         });
            return store;
        }

        /// <summary>
        /// Reads persisted data, calling the provided handler once per item read.
        /// </summary>
        /// <typeparam name="TInternal">Type of the internal data storage class.</typeparam>
        /// <param name="handler">The method to handle read data.</param>
        public void ReadData<TInternal>(DataHandler<TInternal> handler)
            where TInternal : class, IInternalData, new()
        {
            Events.Write.BeginReadPersistedData();

            var keyConverter = new KeyConverter(this.DimensionSet, this.TargetDimensionSet ?? this.DimensionSet);
            try
            {
                using (var dataStore = this.LoadData<TInternal>())
                {
                    foreach (var kvp in dataStore)
                    {
                        handler(keyConverter.Convert(kvp.Key), kvp.Value);
                    }
                }
            }
            catch (ArgumentOutOfRangeException ex)
            {
                throw new PersistedDataException("Unexpected error while reading data.", ex);
            }
            catch (InvalidDataException ex)
            {
                throw new PersistedDataException("Unexpected error while reading data.", ex);
            }
            catch (EndOfStreamException ex)
            {
                throw new PersistedDataException("Unexpected error while reading data.", ex);
            }
            catch (IndexOutOfRangeException ex)
            {
                throw new PersistedDataException("Unexpected error while reading data.", ex);
            }

            Events.Write.EndReadPersistedData();
        }

        public void Dispose()
        {
            this.sourceStream = null;
        }

        public static unsafe uint ReadLegacyStringValue(byte* buffer, long bufferSize, out string value)
        {
            CheckRemainingBufferSize(bufferSize, 0, sizeof(int));

            var length = *(int*)buffer;
            CheckRemainingBufferSize(bufferSize, sizeof(int), length * sizeof(char));

            value = new string((char*)(buffer + sizeof(int)), 0, length).MaybeIntern();

            return (uint)length * sizeof(char) + sizeof(int);
        }

        public static void CheckRemainingBufferSize(long bufferSize, uint currentOffset, int desiredSize)
        {
            if (bufferSize - currentOffset < desiredSize)
            {
                throw new PersistedDataException("Insufficient remaining space in buffer.");
            }
        }
    }
}
