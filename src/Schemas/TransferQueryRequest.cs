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
