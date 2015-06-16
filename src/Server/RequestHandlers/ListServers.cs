// ---------------------------------------------------------------------
// <copyright file="ListServers.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
