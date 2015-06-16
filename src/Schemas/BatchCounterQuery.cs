// ---------------------------------------------------------------------
// <copyright file="BatchCounterQuery.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System;
    using System.Collections.Generic;

    using Bond;

    [Schema]
    public sealed class BatchCounterQuery
    {
        public BatchCounterQuery()
        {
            this.CounterName = string.Empty;
            this.QueryParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            this.UserContext = string.Empty;
        }

        /// <summary>
        /// Counter to query.
        /// </summary>
        [Id(1), Required]
        public string CounterName { get; set; }

        /// <summary>
        /// Query parameters for the counter.
        /// </summary>
        [Id(2), Required, Type(typeof(IDictionary<string, string>))]
        public IDictionary<string, string> QueryParameters { get; set; }

        /// <summary>
        /// Optional context value to associate with each query.
        /// </summary>
        [Id(3)]
        public string UserContext { get; set; }
    }
}
