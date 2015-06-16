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
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;

    using MetricSystem.Server;
    using MetricSystem.Utilities;

    using NUnit.Framework;

    [TestFixture]
    public sealed class RegisterRequestHandlerTests : RequestHandlerTestBase
    {
        [Test]
        public async Task RegisterReturnsErrorIfNotPostRequest()
        {
            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server,
                                                                     RegistrationClient.RegistrationEndpoint,
                                                                     string.Empty));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task RegisterReturnsBadRequestIfNoPayloadDataIsProvided()
        {
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server,
                                                                      RegistrationClient.RegistrationEndpoint,
                                                                      string.Empty),
                                                     new ByteArrayContent(new byte[0]));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task RegisterReturnsBadRequestIfPayloadIsInvalid()
        {
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server,
                                                                      RegistrationClient.RegistrationEndpoint,
                                                                      string.Empty),
                                                     new ByteArrayContent(Encoding.UTF8.GetBytes("yo dawg")));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task StartExecuteReturnsOkAndAddsHandlerIfPayloadIsValid()
        {
            var message = new ServerRegistration
                          {
                              Hostname = "testServer",
                              Port = 867,
                          };
            ByteArrayContent payload;
            using (var ms = new MemoryStream())
            using (var writerStream = new WriterStream(ms))
            {
                writerStream.CreateCompactBinaryWriter().Write(message);
                payload = new ByteArrayContent(ms.GetBuffer());
            }

            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server,
                                                                      RegistrationClient.RegistrationEndpoint,
                                                                      string.Empty),
                                                     payload);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            Assert.IsNotNull(this.server.ServerList["testServer"]);
        }
    }
}
