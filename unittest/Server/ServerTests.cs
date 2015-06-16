// ---------------------------------------------------------------------
// <copyright file="ServerTests.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Server.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading.Tasks;

    using NUnit.Framework;

    [TestFixture]
    public sealed class ServerTests : RequestHandlerTestBase
    {
        sealed class TestRequestHandler : RequestHandler
        {
            public event Func<Request, Response> RequestReceived;

            public override string Prefix
            {
                get { return "/testhandler"; }
            }

            public override Task<Response> ProcessRequest(Request request)
            {
                if (this.RequestReceived != null)
                {
                    return Task.FromResult(this.RequestReceived(request));
                }

                return Task.FromResult(request.CreateErrorResponse(HttpStatusCode.Gone, "Gone fishin'."));
            }
        }

        private TestRequestHandler handler;

        public override IList<RequestHandler> GetRequestHandlers()
        {
            this.handler = new TestRequestHandler();
            return new RequestHandler[] {this.handler};
        }

        [Test]
        public async Task RequestParametersArePassedCorrectly()
        {
            var requestParams = new Dictionary<string, string>
                                {
                                    {"omg", "hai2u"},
                                    {"can-you", "not?"}
                                };

            this.handler.RequestReceived +=
                request =>
                {
                    Assert.AreEqual(requestParams.Count, request.QueryParameters.Count);
                    foreach (var param in requestParams.Keys)
                    {
                        Assert.AreEqual(requestParams[param], request.QueryParameters[param]);
                    }

                    return new Response(request, HttpStatusCode.OK, "Party on, Wayne.");
                };

            var response =
                await this.httpClient.GetAsync(TestUtils.GetUri(this.server, this.handler.Prefix, string.Empty,
                                                                requestParams));

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        }

        [Test]
        public async Task InvalidRequestParametersAreIgnored()
        {
            this.handler.RequestReceived +=
                request =>
                {
                    Assert.AreEqual(0, request.QueryParameters.Count);
                    return new Response(request, HttpStatusCode.OK, "Party on, Wayne.");
                };

            var response =
                await this.httpClient.GetAsync(TestUtils.GetUri(this.server, this.handler.Prefix, string.Empty,
                                                                "?invalid-parameter"));

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        }

        [Test]
        public async Task ContentLengthHeaderIsPopulated()
        {
            this.handler.RequestReceived +=
                request =>
                {
                    Assert.AreEqual(0, request.QueryParameters.Count);
                    return new Response(request, HttpStatusCode.OK, "Party on, Wayne.");
                };

            var response =
                await this.httpClient.GetAsync(TestUtils.GetUri(this.server, this.handler.Prefix, string.Empty));

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            Assert.IsTrue(response.Content.Headers.ContentLength.HasValue);
            Assert.IsTrue(0 < response.Content.Headers.ContentLength.Value);
        }

        [Test]
        public async Task CompressedAndUncompressedResponses()
        {
            this.server.MinimumResponseSizeToCompress = 1 << 20; // not gonna happen.
            this.handler.RequestReceived +=
                request =>
                {
                    Assert.AreEqual(0, request.QueryParameters.Count);
                    return new Response(request, HttpStatusCode.OK, "Party on, Wayne.");
                };

            var response =
                await this.httpClient.GetAsync(TestUtils.GetUri(this.server, this.handler.Prefix, string.Empty));

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            Assert.IsTrue(response.Content.Headers.ContentLength.HasValue);
            Assert.IsFalse(response.Headers.Contains("UncompressedLength"));
            Assert.IsTrue(0 < response.Content.Headers.ContentLength.Value);

            this.server.MinimumResponseSizeToCompress = 0;
            response = await this.httpClient.GetAsync(TestUtils.GetUri(this.server, this.handler.Prefix, string.Empty));

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            Assert.IsTrue(response.Content.Headers.ContentLength.HasValue);
            Assert.IsTrue(response.Headers.Contains("UncompressedLength"));
            Assert.IsTrue(0 < response.Content.Headers.ContentLength.Value);
        }
    }
}
