// ---------------------------------------------------------------------
// <copyright file="TieredRequest.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System;
    using System.Collections.Generic;

    using Bond;

    /// <summary>
    /// Common base class for requests that may be tiered (multi-level fanout)
    /// </summary>
    [Schema]
    public class TieredRequest : ICloneable
    {
        public TieredRequest()
        {
            this.Sources = new List<ServerInfo>();
            this.FanoutTimeoutInMilliseconds = 300;
            this.MaxFanout = 50;
            this.IncludeRequestDiagnostics = true;
        }

        /// <summary>
        /// List of desired additional sources to send this request to. 
        /// If this is not populated it is assumed that the recipient is the only desired source.
        /// </summary>
        [Id(1), Type(typeof(List<ServerInfo>))]
        public IList<ServerInfo> Sources { get; set; }

        /// <summary>
        /// Timeout in milliseconds for queries which require retrieval from multiple sources.
        /// </summary>
        [Id(2)]
        public long FanoutTimeoutInMilliseconds { get; set; }

        /// <summary>
        /// Maximum machines a single client should fan out to
        /// (if number of sources exceeds this number, the client must instruct its downstream servers to fan out further)
        /// </summary>
        [Id(3)]
        public long MaxFanout { get; set; }

        /// <summary>
        /// If true, the response will contain per-machine diagnostics
        /// </summary>
        [Id(4)]
        public bool IncludeRequestDiagnostics { get; set; }

        /// <summary>
        /// Implementation of ICloneable. Should be overwritten by derived classes
        /// </summary>
        public virtual object Clone()
        {
            return Clone(this);
        }

        protected static TData Clone<TData>(TData original)
            where TData : TieredRequest, new()
        {
            var obj = new TData();
            original.CloneProperties(obj);

            return obj;
        }

        protected virtual void CloneProperties(TieredRequest request)
        {
            request.IncludeRequestDiagnostics = this.IncludeRequestDiagnostics;
            request.MaxFanout = this.MaxFanout;
            request.FanoutTimeoutInMilliseconds = this.FanoutTimeoutInMilliseconds;
            request.Sources = new List<ServerInfo>(this.Sources);
        }
    }
}
