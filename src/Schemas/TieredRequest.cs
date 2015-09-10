// The MIT License (MIT)
//
// Copyright (c) 2015 Microsoft
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
