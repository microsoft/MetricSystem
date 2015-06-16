// ---------------------------------------------------------------------
// <copyright file="PersistedDataSource.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using MetricSystem.Utilities;

    /// <summary>
    /// Possible statuses for a given data source.
    /// </summary>
    internal enum PersistedDataSourceStatus
    {
        /// <summary>
        /// The source's status is not currently known.
        /// </summary>
        Unknown,

        /// <summary>
        /// The source data is available.
        /// </summary>
        Available,

        /// <summary>
        /// The source data is unavailable.
        /// </summary>
        Unavailable,

        /// <summary>
        /// The source data is partially available (mix of Available and
        /// Unknown)
        /// </summary>
        Partial
    }

    /// <summary>
    /// Describes a potential source of persisted data.
    /// </summary>
    internal sealed class PersistedDataSource
    {
        public PersistedDataSource(string name, PersistedDataSourceStatus status)
        {
            this.Name = name;
            this.Status = status;
        }

        public PersistedDataSource(BufferReader reader)
        {
            var start = reader.BytesRead;
            this.Name = reader.ReadString();
            this.Status = (PersistedDataSourceStatus)reader.ReadVariableLengthInt32();
            this.SerializedSize = reader.BytesRead - start;
        }

        /// <summary>
        /// Name (typically machine name) of the source.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Whether data from the source is available (meaning it is already
        /// in the persisted data)
        /// </summary>
        public PersistedDataSourceStatus Status { get; set; }

        public long SerializedSize { get; private set; }

        public void Write(BufferWriter writer)
        {
            var startLength = writer.BytesWritten;
            writer.WriteString(this.Name);
            writer.WriteVariableLengthInt32((int)this.Status);
            this.SerializedSize = writer.BytesWritten - startLength;
        }
    }
}