// ---------------------------------------------------------------------
// <copyright file="RegistryRequestHandlerTests.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
