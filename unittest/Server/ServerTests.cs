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
