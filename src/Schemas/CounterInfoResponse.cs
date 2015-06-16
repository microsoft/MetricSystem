// ---------------------------------------------------------------------
// <copyright file="CounterInfoResponse.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System.Collections.Generic;

    using Bond;

    /// <summary>
    /// Server response for data samples
    /// </summary>
    [Schema]
    public class CounterInfoResponse : TieredResponse
    {
        public CounterInfoResponse()
        {
            this.Counters = new List<CounterInfo>();
        }

        /// <summary>
        /// List of names for individual data.
        /// </summary>
        [Id(1), Required, Type(typeof(List<CounterInfo>))]
        public IList<CounterInfo> Counters { get; set; }
    }
}
