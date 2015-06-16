// ---------------------------------------------------------------------
// <copyright file="PersistedData.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System.Collections.Generic;

    using Bond;

    public enum PersistedDataType
    {
        HitCount,
        VariableEncodedHistogram,
        Unknown,
    }

    /// <summary>
    /// Header of persisted data.
    /// </summary>
    [Schema]
    public class PersistedDataVersion
    {
        /// <summary>
        /// Version.
        /// </summary>
        [Id(1), Required]
        public ushort Version { get; set; }
    }

    /// <summary>
    /// Describes the length of content for a block of persisted data.
    /// </summary>
    [Schema]
    public class PersistedDataBlockLength
    {
        /// <summary>
        /// Number of bytes in the block.
        /// </summary>
        [Id(1), Required]
        public long Length { get; set; }
    }

    /// <summary>
    /// Possible statuses for a given data source.
    /// </summary>
    public enum PersistedDataSourceStatus
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
    [Schema]
    public class PersistedDataSource
    {
        public PersistedDataSource()
        {
            this.Name = string.Empty;
            this.Status = PersistedDataSourceStatus.Unknown;
        }

        /// <summary>
        /// Name (typically machine name) of the source.
        /// </summary>
        [Id(1), Required]
        public string Name { get; set; }

        /// <summary>
        /// Whether data from the source is available (meaning it is already
        /// in the persisted data)
        /// </summary>
        [Id(2), Required]
        public PersistedDataSourceStatus Status { get; set; }
    }

    /// <summary>
    /// A collection of persisted data. All data in the collection is expected
    /// to have the same type.
    /// </summary>
    [Schema]
    public class PersistedDataHeader
    {
        public PersistedDataHeader()
        {
            this.Name = string.Empty;
            this.StartTime = string.Empty;
            this.EndTime = string.Empty;
            this.DataType = PersistedDataType.Unknown;
            this.Sources = new List<PersistedDataSource>();
        }

        /// <summary>
        /// The name of the data.
        /// </summary>
        [Id(1), Required]
        public string Name { get; set; }

        /// <summary>
        /// ISO 8601 format timestamp marking when the sample data starts.
        /// </summary>
        [Id(2), Required]
        public string StartTime { get; set; }

        /// <summary>
        /// ISO 8601 format timestamp marking when the sample data ends.
        /// </summary>
        [Id(3), Required]
        public string EndTime { get; set; }

        /// <summary>
        /// The type of the data.
        /// </summary>
        [Id(4), Required]
        public PersistedDataType DataType { get; set; }

        /// <summary>
        /// List of sources which contributed to the data.
        /// </summary>
        [Id(5), Required, Type(typeof(List<PersistedDataSource>))]
        public IList<PersistedDataSource> Sources { get; set; }

        /// <summary>
        /// The number of data elements expected to follow.
        /// </summary>
        [Id(10)]
        public uint DataCount { get; set; }
    }
}
