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
