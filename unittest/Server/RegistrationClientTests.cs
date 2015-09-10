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
    using System.Linq;
    using System.Net;

    using MetricSystem.Data;
    using MetricSystem.Utilities;
    using MetricSystem.Utilities.UnitTests;

    using NUnit.Framework;

    [TestFixture]
    public sealed class RegistrationClientTests
    {
        private TestHttpServer server;
        private DataManager dataManager;
        private const string RandomCounterName = "/Whatever";

        [SetUp]
        public void SetUp()
        {
            this.dataManager = new DataManager();
            var counter = this.dataManager.CreateHitCounter(RandomCounterName, DimensionSet.Empty).Result;
            counter.Increment(42, new DimensionSpecification());

            this.server = new TestHttpServer();
        }

        [TearDown]
        public void TearDown()
        {
            this.server.Shutdown();
        }

        [Test]
        public void TestRegistration()
        {
            const string sourceHostname = "chazwazer"; // see http://simpsons.wikia.com/wiki/Made-up_words#C
            const string sourceMachineFunction = "cromulent";
            const string sourceDatacenter = "unpossible";
            const ushort sourcePort = 867;

            using (
                var client = new RegistrationClient("localhost", (ushort)this.server.Port, sourceHostname, sourcePort,
                    sourceMachineFunction, sourceDatacenter, TimeSpan.FromSeconds(60), this.dataManager))
            {
                client.Start();

                this.server.GotRequest += request =>
                                          {
                                              var uri = request.Request.Url;

                                              Assert.AreEqual(RegistrationClient.RegistrationEndpoint, uri.AbsolutePath);

                                              using (var readStream = new ReaderStream(request.Request.InputStream))
                                              {
                                                  var reader = readStream.CreateCompactBinaryReader();
                                                  var serverRegistration = reader.Read<ServerRegistration>();

                                                  Assert.AreEqual(sourceHostname, serverRegistration.Hostname);
                                                  Assert.AreEqual(sourcePort, serverRegistration.Port);
                                                  Assert.AreEqual(1, serverRegistration.Counters.Count);
                                                  Assert.IsTrue(
                                                                serverRegistration.Counters.Any(
                                                                                                c =>
                                                                                                c.Name.Equals(
                                                                                                              RandomCounterName)));

                                                  return HttpStatusCode.OK;
                                              }
                                          };
            }
        }
    }
}
