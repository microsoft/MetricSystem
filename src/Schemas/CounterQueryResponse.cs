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
