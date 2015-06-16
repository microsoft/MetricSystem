// ---------------------------------------------------------------------
// <copyright file="HttpConfigurationSourceTests.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Configuration.UnitTests
{
    using System;
    using System.Net;
    using System.Threading;

    using MetricSystem.Utilities.UnitTests;
    using NUnit.Framework;

    [TestFixture]
    public sealed class HttpConfigurationSourceTests
    {
        [SetUp]
        public void SetUp()
        {
            this.server = new TestHttpServer();
        }

        [TearDown]
        public void TearDown()
        {
            this.server.Shutdown();
        }

        private TestHttpServer server;

        private static readonly TimeSpan UpdateFrequency = TimeSpan.FromSeconds(1);

        [Test]
        public void CanRetrieveBasicData()
        {
            var gotUpdate = new ManualResetEventSlim(false);
            this.server.GotRequest +=
                ctx =>
                {
                    this.server.ResponseMessage = "867-5309 (JENNY)";
                    return HttpStatusCode.OK;
                };

            using (var source = new HttpConfigurationSource("http://localhost:" + this.server.Port, UpdateFrequency))
            {
                source.SourceUpdated += gotUpdate.Set;
                Assert.IsTrue(gotUpdate.Wait(TimeSpan.FromSeconds(10)));
            }
        }

        [Test]
        public void DataIsUpdatedIfChanged()
        {
            var responses = new[] {"I am Groot", "I AM A KEYTREE"};
            int currentResponse = 0;

            var gotUpdate = new ManualResetEventSlim(false);
            this.server.GotRequest +=
                ctx =>
                {
                    this.server.ResponseMessage = responses[currentResponse];
                    // We need to add this header to avoid default caching behavior.
                    ctx.Response.AddHeader("Cache-Control", "no-cache");
                    ++currentResponse;
                    if (currentResponse == responses.Length)
                    {
                        currentResponse = 0;
                    }
                    return HttpStatusCode.OK;
                };

            using (var source = new HttpConfigurationSource("http://localhost:" + this.server.Port, UpdateFrequency))
            {
                source.SourceUpdated += gotUpdate.Set;
                Assert.IsTrue(gotUpdate.Wait(TimeSpan.FromSeconds(10)));
                gotUpdate.Reset();
                Assert.IsTrue(gotUpdate.Wait(TimeSpan.FromSeconds(10)));
            }
        }

        [Test]
        public void SourceDoesNotUpdateIfServerRespondsWithError()
        {
            var gotRequest = new ManualResetEventSlim(false);
            this.server.GotRequest +=
                ctx =>
                {
                    this.server.ResponseMessage = string.Empty;
                    gotRequest.Set();
                    return HttpStatusCode.NotFound;
                };

            using (var source = new HttpConfigurationSource("http://localhost:" + this.server.Port, UpdateFrequency))
            {
                var updated = false;
                source.SourceUpdated += () => updated = true;
                Assert.IsTrue(gotRequest.Wait(TimeSpan.FromSeconds(10)));
                Assert.IsFalse(updated);
            }
        }
    }
}
