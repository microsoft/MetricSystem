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
