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
