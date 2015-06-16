// ---------------------------------------------------------------------
// <copyright file="ListServersRequestHandlerTests.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Server.UnitTests
{
    using System.Net;
    using System.Threading.Tasks;

    using MetricSystem.Server.RequestHandlers;
    using MetricSystem.Utilities;

    using NUnit.Framework;

    [TestFixture]
    public class ListServersRequestHandlerTests : RequestHandlerTestBase
    {
        [Test]
        public async Task ListServersRequestHandlerReturnsErrorIfKnownHostsIsEmpty()
        {
            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server,
                                                                     ListServersRequestHandler.CommandPrefix,
                                                                     string.Empty));
            Assert.AreEqual(HttpStatusCode.NotFound, response.StatusCode);
        }

        [Test]
        public async Task ListServersRequestHandlerReturnsValueIfKnownHostsIsNotEmpty()
        {
            var serverList = this.server.ServerList;
            const string hostname = "host";
            const ushort port = 1;
            serverList.InsertOrUpdate(new ServerRegistration {Hostname = hostname, Port = port});

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server,
                                                                     ListServersRequestHandler.CommandPrefix,
                                                                     string.Empty));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<ListServerResponse>();
                var res = reader.Deserialize();

                Assert.AreEqual(1, res.Servers.Count);
                var server = res.Servers[0];
                Assert.AreEqual(hostname, server.Hostname);
                Assert.AreEqual(port, server.Port);
            }
        }
    }
}
