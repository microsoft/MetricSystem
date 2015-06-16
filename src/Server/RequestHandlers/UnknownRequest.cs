// ---------------------------------------------------------------------
// <copyright file="UnknownRequest.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Server.RequestHandlers
{
    using System.Net;
    using System.Threading.Tasks;

    /// <summary>
    /// Handler for unknown or invalid requests.
    /// </summary>
    internal sealed class UnknownRequestHandler : RequestHandler
    {
        internal const string Path = "/";
        public override string Prefix
        {
            get { return Path; }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public override Task<Response> ProcessRequest(Request request)
        {
            return Task.FromResult(request.CreateErrorResponse(HttpStatusCode.NotFound, "Unknown command."));
        }
    }
}
