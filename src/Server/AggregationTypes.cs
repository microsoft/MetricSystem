// ---------------------------------------------------------------------
// <copyright file="AggregationTypes.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Server
{
    using System;

    /// <summary>
    /// Types of aggregation supported by the server.
    /// </summary>
    [Flags]
    public enum AggregationTypes
    {
        None = 0x0,
        /// <summary>
        /// Enable pre-aggregation of counter data by the server.
        /// </summary>
        PreAggregate = 0x1,
        /// <summary>
        /// Enable queries to the server with no specified sources to be sent to all known downstream providers.
        /// </summary>
        QueryAggregate = 0x2
    }
}
