// ---------------------------------------------------------------------
// <copyright file="CounterQueryResponse.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System.Collections.Generic;

    using Bond;
    using Bond.Tag;

    /// <summary>
    /// Response for a single counter query (potentially embedded in a batch response)
    /// </summary>
    [Schema]
    public class CounterQueryResponse : TieredResponse
    {
        public CounterQueryResponse()
        {
            this.UserContext = string.Empty;
            this.ErrorMessage = string.Empty;
        }

        /// <summary>
        /// Context string echoed back to the caller of the batch request
        /// </summary>
        [Id(1)]
        public string UserContext { get; set; }

        /// <summary>
        /// HttpStatusCode for this individual counter request
        /// </summary>
        [Id(2)]
        public short HttpResponseCode { get; set; }

        /// <summary>
        /// Descriptive message in case of error
        /// </summary>
        [Id(3)]
        public string ErrorMessage { get; set; }

        /// <summary>
        /// Payload for successful requests
        /// </summary>
        [Id(4), Required, Type(typeof(nullable<List<DataSample>>))]
        public IList<DataSample> Samples { get; set; }
    }
}
