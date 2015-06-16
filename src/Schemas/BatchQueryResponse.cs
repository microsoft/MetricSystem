// ---------------------------------------------------------------------
// <copyright file="BatchQueryResponse.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System.Collections.Generic;

    using Bond;

    /// <summary>
    /// [V2 schema] Response schema received upon making a batch response
    /// </summary>
    [Schema]
    public class BatchQueryResponse : TieredResponse
    {
        public BatchQueryResponse()
        {
            this.Responses = new List<CounterQueryResponse>();
        }

        /// <summary>
        /// Collection of Responses
        /// </summary>
        [Id(1), Required, Type(typeof(List<CounterQueryResponse>))]
        public IList<CounterQueryResponse> Responses { get; set; }
    }
}
