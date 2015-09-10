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
    /// Details of MetricSystem server registered with aggregation server
    /// </summary>
    [Schema]
    public class ServerRegistration
    {
        public ServerRegistration()
        {
            this.Hostname = string.Empty;
            this.MachineFunction = string.Empty;
            this.Datacenter = string.Empty;
            this.Counters = new List<CounterInfo>();
        }

        /// <summary>
        /// Server hostname.
        /// </summary>
        [Id(1), Required]
        public string Hostname { get; set; }

        /// <summary>
        /// Server port number.
        /// </summary>
        [Id(2), Required]
        public ushort Port { get; set; }

        /// <summary>
        /// Machine function.
        /// </summary>
        [Id(3), Required]
        public string MachineFunction { get; set; }

        /// <summary>
        /// Datacenter the server is registering from.
        /// </summary>
        [Id(4), Required]
        public string Datacenter { get; set; }

        /// <summary>
        /// Counters hosted by the registering server.
        /// </summary>
        [Id(5), Type(typeof(List<CounterInfo>))]
        public IList<CounterInfo> Counters;
    }
}
