// ---------------------------------------------------------------------
// <copyright file="ListServerResponse.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System.Collections.Generic;

    using Bond;

    /// <summary>
    /// List of Metric System servers registered with the aggregation Server
    /// </summary>
    [Schema]
    public class ListServerResponse
    {
        public ListServerResponse()
        {
            this.Servers = new List<ServerInfo>();
        }

        [Id(1), Required, Type(typeof(List<ServerInfo>))]
        public IList<ServerInfo> Servers { get; set; }
    }
}
