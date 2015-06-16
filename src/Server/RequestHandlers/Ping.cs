// ---------------------------------------------------------------------
// <copyright file="Ping.cs" company="Microsoft">
//       Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Server.RequestHandlers
{
    using System.Net;
    using System.Threading.Tasks;

    /// <summary>
    /// Basic handler to respond with status information.
    /// </summary>
    internal sealed class PingRequestHandler : RequestHandler
    {
        internal const string ResponseMessage = "Service is available.";
        internal const string ResponseType = "text/plain";

        public override string Prefix
        {
            get { return "/ping"; }
        }

        public override Task<Response> ProcessRequest(Request request)
        {
            return
                Task.FromResult(new Response(request, HttpStatusCode.OK, ResponseMessage) {ContentType = ResponseType});
        }
    }
}
