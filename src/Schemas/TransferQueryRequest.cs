// ---------------------------------------------------------------------
// <copyright file="TransferQueryRequest.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System.Collections.Generic;

    using Bond;

    /// <summary>
    /// [Legacy] Request body for a persisted data transfer.
    /// </summary>
    [Schema]
    public class TransferRequest
    {
        public TransferRequest()
        {
            this.DataType = PersistedDataType.Unknown;
            this.Timeout = 300;
            this.MaxFanout = 50;
            this.Sources = new List<string>();
        }

        /// <summary>
        /// Type of data being requested.
        /// </summary>
        [Id(1), Required]
        public PersistedDataType DataType { get; set; }

        /// <summary>
        /// Timeout in seconds for queries which require retrieval from multiple sources.
        /// </summary>
        [Id(2)]
        public long Timeout { get; set; }

        /// <summary>
        /// Maximum machines to fan out to.
        /// </summary>
        [Id(3)]
        public long MaxFanout { get; set; }

        /// <summary>
        /// List of desired sources. If this is not populated it is assumed
        /// that the recipient is the only desired source.
        /// </summary>
        [Id(10), Type(typeof(List<string>))]
        public IList<string> Sources { get; set; }
    }

    /// <summary>
    /// [V2 query] Request body for a persisted data transfer.
    /// </summary>
    [Schema]
    public class TransferQueryRequest : TieredRequest
    {
        public TransferQueryRequest()
        {
            this.DataType = PersistedDataType.Unknown;
        }

        /// <summary>
        /// Type of data being requested.
        /// </summary>
        [Id(1), Required]
        public PersistedDataType DataType { get; set; }

        public override object Clone()
        {
            return Clone(this);
        }

        protected override void CloneProperties(TieredRequest request)
        {
            base.CloneProperties(request);

            var clone = (TransferQueryRequest)request;
            clone.DataType = this.DataType;
        }
    }
}
