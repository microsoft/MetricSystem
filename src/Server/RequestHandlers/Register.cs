// ---------------------------------------------------------------------
// <copyright file="Register.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Server.RequestHandlers
{
    using System.Net;
    using System.Threading.Tasks;

    internal sealed class RegisterRequestHandler : RequestHandler
    {
        private readonly ServerList serverList;

        public RegisterRequestHandler(ServerList serverList)
        {
            this.serverList = serverList;
        }

        public override string Prefix
        {
            get { return RegistrationClient.RegistrationEndpoint; }
        }

        public override async Task<Response> ProcessRequest(Request request)
        {
            if (!request.HasInputBody)
            {
                return request.CreateErrorResponse(HttpStatusCode.BadRequest, "no input body provided.");
            }

            var registrationMessage = await request.ReadInputBody<ServerRegistration>();
            if (registrationMessage != null)
            {
                var server = this.serverList.InsertOrUpdate(registrationMessage);
                server.ProcessRegistrationMessage(registrationMessage);
                return new Response(request, HttpStatusCode.OK, "Registered.");
            }

            return request.CreateErrorResponse(HttpStatusCode.BadRequest, "Invalid data.");
        }
    }
}
