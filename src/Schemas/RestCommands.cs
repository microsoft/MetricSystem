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
    public static class RestCommands
    {
        /// <summary>
        /// POST command. Request body is BatchQueryRequest. Response is BatchQueryResponse
        /// </summary>
        public const string BatchQueryCommand = "/batch";

        /// <summary>
        /// POST with a CounterWriteRequest body to write one or more values into a single machine. Fanout is not
        /// supported. The counter name should be specified after write (e.g. /write/Some/Counter)
        /// </summary>
        public const string CounterWriteCommand = "/write";

        /// <summary>
        /// Root Prefix of all counter requests (list all counters, list dimensions, query, etc). 
        /// Subcommands can be GET or POST. No subcommand requires a POST with a CounterQueryRequest body
        /// </summary>
        public const string CounterRequestCommand = "/counters";

        /// <summary>
        /// GET will query information for counters on this machine as a CounterInfoResponse.
        /// POST will fan out to other machines. POST body is a TieredRequest, response is AggregatedListDataResponse.
        /// </summary>
        public const string CounterInfoCommand = "info";

        /// <summary>
        /// GET for basic query to a single machine with dimension filtering /counters/&lt;pattern&gt;/query?dim1=12;dim2=abc
        /// POST can fan out to other machines. POST body is CounterQueryRequest body.
        /// </summary>
        public const string CounterQueryCommand = "query";
    }
}
