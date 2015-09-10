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

namespace MetricSystem.Server.RequestHandlers
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Net;
    using System.Threading.Tasks;

    using MetricSystem.Server;

    /// <summary>
    /// returns the list of metric system servers registered to the aggregation server
    /// </summary>
    internal sealed class ListServersRequestHandler : RequestHandler
    {
        internal const string CommandPrefix = "/listServers";
        private readonly ServerList serverList;

        public ListServersRequestHandler(ServerList serverList)
        {
            this.serverList = serverList;
        }

        public override string Prefix
        {
            get { return CommandPrefix; }
        }

        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public override Task<Response> ProcessRequest(Request request)
        {
            // The '.Servers' property actually returns a new list so this is totally reasonable.
            var response = new ListServerResponse {Servers = this.serverList.Servers};
            if (response.Servers.Count == 0)
            {
                return Task.FromResult(request.CreateErrorResponse(HttpStatusCode.NotFound, "No known servers."));
            }

            return Task.FromResult(Response.Create(request, HttpStatusCode.OK, response));
        }
    }
}
