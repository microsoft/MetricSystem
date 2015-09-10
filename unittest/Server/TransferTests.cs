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
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;

    using Microsoft.IO;
    using MetricSystem.Data;
    using MetricSystem.Server.RequestHandlers;

    using NUnit.Framework;

    [TestFixture]
    public sealed class TransferRequestHandlerTests : RequestHandlerTestBase
    {
        private DimensionSet dimensionSet;
        private const string AnyDimension = "dim";
        private RecyclableMemoryStreamManager memoryStreamManager;

        private DateTime GetStartTime()
        {
            return (this.dataManager.GetCounter<Counter>(AnyCounter)).GetDimensionValues(ReservedDimensions.StartTimeDimension,
                                                                                         new DimensionSpecification())
                                                                     .Select(DateTime.Parse)
                                                                     .Min();
        }

        private DateTime GetEndTime()
        {
            // This helps us get the *sealed* end time, the final time is going to be unsealed data.
            var times =
                (this.dataManager.GetCounter<Counter>(AnyCounter)).GetDimensionValues(ReservedDimensions.EndTimeDimension,
                                                                                      new DimensionSpecification())
                                                                  .Select(DateTime.Parse)
                                                                  .ToList();
            times.Sort();
            return times[times.Count - 2];
        }

        public override void Setup()
        {
            this.dataManager.CompactionConfiguration =
                new DataCompactionConfiguration(new[]
                                         {
                                             new DataIntervalConfiguration(TimeSpan.FromMinutes(5), TimeSpan.MaxValue), 
                                         });
            this.dataManager.MaximumDataAge = TimeSpan.Zero; // Don't want data getting deleted here.

            // Make a counter and write some stuff.
            this.dimensionSet = new DimensionSet(new HashSet<Dimension> { new Dimension(AnyDimension), });
            var counter = this.dataManager.CreateHitCounter(AnyCounter, this.dimensionSet).Result; 

            var dims = new DimensionSpecification();
            var timestamp = DateTime.Now;
            dims[AnyDimension] = "a";
            counter.Increment(dims, timestamp);
            dims[AnyDimension] = "b";
            counter.Increment(2, dims, timestamp);
            dims[AnyDimension] = "c";
            counter.Increment(dims, timestamp);

            // We need to force a seal by setting some value waaaay in the future.
            counter.Increment(dims, timestamp.AddYears(10));

            this.memoryStreamManager = new RecyclableMemoryStreamManager(1 << 17, 1 << 20, 1 << 24);
        }

        [Test]
        public void ConstructorThrowsArgumentNullExceptionIfDataManagerIsNull()
        {
            try
            {
                new TransferRequestHandler(null);
                Assert.Fail();
            }
            catch (ArgumentNullException) { }
        }

        [Test]
        public async Task EmptyRequestParametersReturnsBadRequest()
        {
            var response =
                await this.httpClient.GetAsync(TestUtils.GetUri(this.server, TransferRequestHandler.CommandPrefix,
                                                                AnyCounter));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task MissingStartRequestParametersReturnsBadRequest()
        {
            var response =
                await this.httpClient.GetAsync(TestUtils.GetUri(this.server, TransferRequestHandler.CommandPrefix,
                                                                AnyCounter, new[]
                                                                            {
                                                                                new KeyValuePair<string, string>(
                                                                                    ReservedDimensions.EndTimeDimension,
                                                                                    this.GetEndTime().ToString()),
                                                                            }));

            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task MissingEndRequestParametersReturnsBadRequest()
        {
            var response =
                await this.httpClient.GetAsync(TestUtils.GetUri(this.server, TransferRequestHandler.CommandPrefix,
                                                                AnyCounter, new[]
                                                                            {
                                                                                new KeyValuePair<string, string>(
                                                                                    ReservedDimensions.StartTimeDimension,
                                                                                    this.GetStartTime().ToString()),
                                                                            }));

            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task TooManyDimensionsReturnsBadRequest()
        {
            var response =
                await this.httpClient.GetAsync(TestUtils.GetUri(this.server,
                                                                TransferRequestHandler.CommandPrefix,
                                                                AnyCounter, new[]
                                                                            {
                                                                                new KeyValuePair<string, string>(
                                                                                    ReservedDimensions.StartTimeDimension,
                                                                                    this.GetStartTime().ToString()),
                                                                                new KeyValuePair<string, string>(
                                                                                    ReservedDimensions.EndTimeDimension,
                                                                                    this.GetEndTime().ToString()),
                                                                                new KeyValuePair<string, string>(
                                                                                    "foo", "bar"),
                                                                            }));

            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task StartAndEndRequestParametersReturnsOK()
        {
            var response =
                await this.httpClient.GetAsync(TestUtils.GetUri(this.server,
                                                                TransferRequestHandler.CommandPrefix,
                                                                AnyCounter, new[]
                                                                            {
                                                                                new KeyValuePair<string, string>(
                                                                                    ReservedDimensions.StartTimeDimension,
                                                                                    this.GetStartTime().ToString()),
                                                                                new KeyValuePair<string, string>(
                                                                                    ReservedDimensions.EndTimeDimension,
                                                                                    this.GetEndTime().ToString()),
                                                                            }));

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        }

        [Test]
        public async Task UnknownCounterReturnsResourceNotFound()
        {
            var response =
                await this.httpClient.GetAsync(TestUtils.GetUri(this.server,
                                                                TransferRequestHandler.CommandPrefix,
                                                                "/Some/Random/Data", new[]
                                                                            {
                                                                                new KeyValuePair<string, string>(
                                                                                    ReservedDimensions.StartTimeDimension,
                                                                                    this.GetStartTime().ToString()),
                                                                                new KeyValuePair<string, string>(
                                                                                    ReservedDimensions.EndTimeDimension,
                                                                                    this.GetEndTime().ToString()),
                                                                            }));

            Assert.AreEqual(HttpStatusCode.NotFound, response.StatusCode);
        }

        [Test]
        public async Task RetrievedDataIsPersistedDataReadable()
        {
            var response =
                await this.httpClient.GetAsync(TestUtils.GetUri(this.server,
                                                                TransferRequestHandler.CommandPrefix,
                                                                AnyCounter, new[]
                                                                            {
                                                                                new KeyValuePair<string, string>(
                                                                                    ReservedDimensions.StartTimeDimension,
                                                                                    this.GetStartTime().ToString()),
                                                                                new KeyValuePair<string, string>(
                                                                                    ReservedDimensions.EndTimeDimension,
                                                                                    this.GetEndTime().ToString()),
                                                                            }));

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            var responseStream = await response.Content.ReadAsStreamAsync();
            this.ValidateStreamContents(responseStream);
        }

        [Test]
        public async Task BasicPersistedDataAggregatorReturnsData()
        {
            var aggregator = new PersistedDataAggregator<InternalHitCount>(
                AnyCounter, null, new[] {"localhost"}, this.GetStartTime(), this.GetEndTime(),
                this.memoryStreamManager);
            aggregator.Port = this.server.Port;

            await aggregator.Run();
            Assert.AreEqual(1, aggregator.Sources.Count);
            Assert.AreEqual(PersistedDataSourceStatus.Available, aggregator.Sources[0].Status);
            using (var ms = new MemoryStream())
            {
                aggregator.WriteToStream(ms);
                ms.Position = 0;
                this.ValidateStreamContents(ms);
            }
        }

        private void ValidateStreamContents(Stream input)
        {
            var reader = new PersistedDataReader(input, this.memoryStreamManager);
            Assert.IsTrue(reader.ReadDataHeader());
            Assert.AreEqual((uint)3, reader.Header.DataCount);
            Assert.AreEqual(AnyCounter, reader.Header.Name);
            Assert.AreEqual(this.GetStartTime().ToUniversalTime(), reader.StartTime);
            Assert.AreEqual(this.GetEndTime().ToUniversalTime(), reader.EndTime);

            reader.ReadData<InternalHitCount>(
                (key, data) =>
                {
                    bool allDimensionsProvided;
                    if (
                        key.Matches(this.dimensionSet.CreateKey(new DimensionSpecification {{AnyDimension, "a"}},
                            out allDimensionsProvided)))
                    {
                        Assert.AreEqual((ulong)1, data.HitCount);
                    }
                    else if (
                        key.Matches(this.dimensionSet.CreateKey(
                            new DimensionSpecification {{AnyDimension, "b"}}, out allDimensionsProvided)))
                    {
                        Assert.AreEqual((ulong)2, data.HitCount);
                    }
                    else if (
                        key.Matches(
                            this.dimensionSet.CreateKey(new DimensionSpecification {{AnyDimension, "c"}},
                                out allDimensionsProvided)))
                    {
                        Assert.AreEqual((ulong)1, data.HitCount);
                    }
                    else
                    {
                        Assert.Fail("Unexpected data found!");
                    }
                });
        }
    }
}
