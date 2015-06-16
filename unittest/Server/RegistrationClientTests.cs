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
