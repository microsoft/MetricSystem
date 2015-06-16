// ---------------------------------------------------------------------
// <copyright file="TieredResponse.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System.Collections.Generic;

    using Bond;

    /// <summary>
    /// Base type for any response type returned from a TieredRequest
    /// </summary>
    [Schema]
    public class TieredResponse : MetricSystemResponse
    {
        public TieredResponse()
        {
            this.RequestDetails = new List<RequestDetails>();
        }

        /// <summary>
        /// Optional aggregated response details from each server in the fanout
        /// (if requested)
        /// </summary>
        [Id(1), Type(typeof(List<RequestDetails>))]
        public IList<RequestDetails> RequestDetails { get; set; }
    }
}
