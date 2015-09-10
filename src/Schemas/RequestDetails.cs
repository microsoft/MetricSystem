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
    using Bond;

    /// <summary>
    /// Diagnostic data about an individual response sent to the remote MS server
    /// </summary>
    [Schema]
    public class RequestDetails
    {
        public RequestDetails()
        {
            this.Server = new ServerInfo();
            this.Status = RequestStatus.Success;
            this.StatusDescription = string.Empty;
        }

        /// <summary>
        /// Server name/address
        /// </summary>
        [Id(1)]
        public ServerInfo Server { get; set; }

        /// <summary>
        /// Did the client request succeed or fail?
        /// </summary>
        [Id(2)]
        public RequestStatus Status { get; set; }

        /// <summary>
        /// Optional. description relevant to the status
        /// </summary>
        [Id(3)]
        public string StatusDescription { get; set; }

        /// <summary>
        /// .NET HttpResponseCode from the server
        /// </summary>
        [Id(4)]
        public short HttpResponseCode { get; set; }

        /// <summary>
        /// Was this host an aggregator node? (Did it fan out to any other machines?)
        /// </summary>
        [Id(5)]
        public bool IsAggregator { get; set; }
    }
}
