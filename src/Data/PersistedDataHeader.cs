// ---------------------------------------------------------------------
// <copyright file="PersistedDataTypes.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    using MetricSystem.Utilities;

    using VariableLengthEncoding = MetricSystem.Utilities.ByteConverter.VariableLengthEncoding;

    /// <summary>
    /// Header data for a block of persisted data. Provides 
    /// </summary>
    internal sealed class PersistedDataHeader
    {
        public PersistedDataHeader(string name, DateTime start, DateTime end, PersistedDataType dataType,
                                   IEnumerable<PersistedDataSource> sources, DimensionSet dimensionSet,
                                   uint dataCount)
        {
            this.Name = name;
            this.StartTime = new DateTimeOffset(start, TimeSpan.Zero);
            this.EndTime = new DateTimeOffset(end, TimeSpan.Zero);
            this.DataType = dataType;
            this.Sources = new List<PersistedDataSource>(sources);
            this.DimensionSet = dimensionSet;
            this.DataCount = dataCount;
        }

        public PersistedDataHeader(BufferReader reader)
        {
            var start = reader.BytesRead;

            this.Name = reader.ReadString();

            this.StartTime = reader.ReadVariableLengthInt64().ToDateTimeOffset();
            this.EndTime = reader.ReadVariableLengthInt64().ToDateTimeOffset();

            this.DataType = (PersistedDataType)reader.ReadVariableLengthInt32();
            var sourceCount = reader.ReadVariableLengthInt32();

            this.Sources = new List<PersistedDataSource>(sourceCount);
            for (var i = 0; i < sourceCount; ++i)
            {
                this.Sources.Add(new PersistedDataSource(reader));
            }

            this.DimensionSet = new DimensionSet(reader);

            this.DataCount = reader.ReadVariableLengthUInt32();

            this.SerializedSize = reader.BytesRead - start;
        }

        public void Write(BufferWriter writer)
        {
            var start = writer.BytesWritten;
            writer.WriteString(this.Name);
            writer.WriteVariableLengthInt64(this.StartTime.ToMillisecondTimestamp());
            writer.WriteVariableLengthInt64(this.EndTime.ToMillisecondTimestamp());
            writer.WriteVariableLengthInt32((int)this.DataType);
            writer.WriteVariableLengthInt32(this.Sources.Count);

            foreach (var source in this.Sources)
            {
                source.Write(writer);
            }
            this.DimensionSet.Write(writer);
            writer.WriteVariableLengthUInt32(this.DataCount);

            this.SerializedSize = writer.BytesWritten - start;
        }

        /// <summary>
        /// The name of the data.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Timestamp marking when the sample data starts.
        /// </summary>
        public DateTimeOffset StartTime { get; private set; }

        /// <summary>
        /// ISO 8601 format timestamp marking when the sample data ends.
        /// </summary>
        public DateTimeOffset EndTime { get; private set; }

        /// <summary>
        /// The type of the data.
        /// </summary>
        public PersistedDataType DataType { get; private set; }

        /// <summary>
        /// List of sources which contributed to the data.
        /// </summary>
        public IList<PersistedDataSource> Sources { get; private set; }

        /// <summary>
        /// DimensionSet for this block of data.
        /// </summary>
        public DimensionSet DimensionSet { get; private set; }

        /// <summary>
        /// The number of data elements expected to follow.
        /// </summary>
        public uint DataCount { get; private set; }

        /// <summary>
        /// Serialized size of the header data.
        /// </summary>
        public long SerializedSize { get; set; }
    }
}
