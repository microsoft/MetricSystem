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
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Text;
    using System.Threading.Tasks;

    using Bond.Protocols;

    using MetricSystem.Data;
    using MetricSystem.Utilities;

    using NUnit.Framework;

    [TestFixture]
    public sealed class WriteRequestHandlerTests : RequestHandlerTestBase
    {
        private static ByteArrayContent CreateWriteBody(IEnumerable<CounterWriteOperation> operations)
        {
            var writeBody = new CounterWriteRequest {Writes = operations.ToList()};
            using (var ms = new MemoryStream())
            {
                var writer = new SimpleJsonWriter(ms);
                writer.Write(writeBody);

                var bytes = new byte[ms.Length];
                Buffer.BlockCopy(ms.GetBuffer(), 0, bytes, 0, bytes.Length);

                var content = new ByteArrayContent(bytes);
                content.Headers.ContentType = new MediaTypeHeaderValue(Protocol.ApplicationJsonMimeType);
                return content;
            }
        }

        private static ByteArrayContent CreateSimpleWriteBody()
        {
            return CreateWriteBody(new[] {new CounterWriteOperation {Value = 1}});
        }

        private DataSample QueryCounter<TCounter>(string counterName, DimensionSpecification dims = null)
            where TCounter : Counter
        {
            var counter = this.dataManager.GetCounter<TCounter>(counterName);
            if (counter == null)
            {
                return null;
            }

            counter.DataSet.Flush();

            if (dims == null)
            {
                dims = new DimensionSpecification();
            }
            dims[ReservedDimensions.AggregateSamplesDimension] = "true";
            dims[ReservedDimensions.StartTimeDimension] = DateTime.MinValue.ToString("o");
            dims[ReservedDimensions.EndTimeDimension] = DateTime.MaxValue.ToString("o");

            var samples = counter.Query(dims).ToList();
            return samples.Count == 0 ? null : samples[0];
        }

        public override void Setup()
        {
            this.dataManager.CreateHitCounter(AnyCounter, DimensionSet.Empty).Wait();
        }

        [Test]
        public async Task BadRequestReturnedIfNoInputProvided()
        {
            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterWriteCommand,
                                                                     AnyCounter));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task BadRequestReturnedIfNoCounterNameProvided()
        {
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterWriteCommand,
                                                                      string.Empty),
                                                     CreateSimpleWriteBody());
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task NotFoundRequestReturnedIfCounterIsUnknown()
        {
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterWriteCommand,
                                                                      "/Unknown/Counter"),
                                                     CreateSimpleWriteBody());
            Assert.AreEqual(HttpStatusCode.NotFound, response.StatusCode);
        }

        [Test]
        public async Task BadRequestReturnedIfPayloadInvalid()
        {
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterWriteCommand,
                                                                      AnyCounter),
                                                     new ByteArrayContent(Encoding.UTF8.GetBytes("yo dawg")));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task BadRequestReturnedIfPayloadHasNoOperations()
        {
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterWriteCommand,
                                                                      AnyCounter),
                                                     CreateWriteBody(new CounterWriteOperation[0]));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task BadRequestReturnedIfPayloadHasOperationsWithCountLessThanOne()
        {
            var ops = new[]
                      {
                          new CounterWriteOperation {Value = 1, Count = 867},
                          new CounterWriteOperation {Value = 1, Count = 0},
                          new CounterWriteOperation {Value = 1, Count = 5309}
                      };
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterWriteCommand,
                                                                      AnyCounter),
                                                     CreateWriteBody(ops));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test] // Note this is the only test that should successfully write to AnyCounter or weird things will happen
        public async Task BasicWriteOperationsSucceed()
        {
            var data = this.QueryCounter<HitCounter>(AnyCounter);
            Assert.IsNull(data);
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterWriteCommand,
                                                                      AnyCounter),
                                                     CreateSimpleWriteBody());
            Assert.AreEqual(HttpStatusCode.Accepted, response.StatusCode);

            data = this.QueryCounter<HitCounter>(AnyCounter);
            Assert.AreEqual(1, data.HitCount);
            var time = DateTime.UtcNow;
            var counterTime = data.StartTime.ToDateTime();
            // There should be some delta but it shouldn't be insane. Hate timing tests.
            var diff = time - counterTime;
            Assert.That(diff.TotalHours <= 0.5);
        }

        [Test]
        public async Task DimensionValuesAreHonored()
        {
            const string counterName = "/DimensionValues";
            const string dim1 = "dim1";
            const string dim2 = "dim2";
            await
                this.dataManager.CreateHitCounter(counterName,
                                                  new DimensionSet(
                                                      new HashSet<Dimension>(new[]
                                                                             {new Dimension(dim1), new Dimension(dim2),})));

            var op = new CounterWriteOperation {Value = 42};
            op.DimensionValues.Add(dim1, "867");
            op.DimensionValues.Add(dim2, "5309");
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterWriteCommand,
                                                                      counterName),
                                                     CreateWriteBody(new[] {op}));
            Assert.AreEqual(HttpStatusCode.Accepted, response.StatusCode);

            var data = this.QueryCounter<HitCounter>(counterName, new DimensionSpecification
                                                                  {{dim1, "867"}, {dim2, "5309"}});
            Assert.AreEqual(42, data.HitCount);
        }

        [Test]
        public async Task TimestampsAreHonored()
        {
            const string counterName = "/Timestamps";
            var writeTimestamp = new DateTime(2112, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            await this.dataManager.CreateHitCounter(counterName, DimensionSet.Empty);

            var ops = new[]
                      {
                          new CounterWriteOperation
                          {
                              Value = 1,
                              Timestamp = writeTimestamp.ToMillisecondTimestamp()
                          }
                      };
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterWriteCommand,
                                                                      counterName),
                                                     CreateWriteBody(ops));
            Assert.AreEqual(HttpStatusCode.Accepted, response.StatusCode);

            var data = this.QueryCounter<HitCounter>(counterName);
            Assert.AreEqual(1, data.HitCount);
            Assert.AreEqual(writeTimestamp.ToMillisecondTimestamp(), data.StartTime);
        }

        [Test]
        public async Task ValuesAreWrittenAsCountMultiplesForHitCounts()
        {
            const string counterName = "/HitCountMultiValueCount";
            await this.dataManager.CreateHitCounter(counterName, DimensionSet.Empty);

            var ops = new[]
                      {
                          new CounterWriteOperation
                          {
                              Value = 867,
                              Count = 5309
                          }
                      };
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterWriteCommand,
                                                                      counterName),
                                                     CreateWriteBody(ops));
            Assert.AreEqual(HttpStatusCode.Accepted, response.StatusCode);

            var data = this.QueryCounter<HitCounter>(counterName);
            Assert.AreEqual(867 * 5309, data.HitCount);
        }

        [Test]
        public async Task ValuesAreWrittenAsMultipleHitsForSameValueForHistogram()
        {
            const string counterName = "/HistogramMultiValueCount";
            await this.dataManager.CreateHistogramCounter(counterName, DimensionSet.Empty);

            var ops = new[]
                      {
                          new CounterWriteOperation
                          {
                              Value = 867,
                              Count = 5309
                          }
                      };
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterWriteCommand,
                                                                      counterName),
                                                     CreateWriteBody(ops));
            Assert.AreEqual(HttpStatusCode.Accepted, response.StatusCode);

            var data = this.QueryCounter<HistogramCounter>(counterName);
            Assert.AreEqual(5309, data.Histogram[867]);
        }

        [Test]
        public async Task MultipleWriteOperationsAreProcessedCorrectly()
        {
            const string counterName = "/MultiWriteOperations";
            const string dimensionName = "dim";
            await
                this.dataManager.CreateHitCounter(counterName,
                                                  new DimensionSet(
                                                      new HashSet<Dimension>(new[] {new Dimension(dimensionName)})));

            var ops = new[] {new CounterWriteOperation {Value = 867}, new CounterWriteOperation {Value = 5309}};
            ops[0].DimensionValues.Add(dimensionName, "0");
            ops[1].DimensionValues.Add(dimensionName, "1");
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterWriteCommand,
                                                                      counterName),
                                                     CreateWriteBody(ops));
            Assert.AreEqual(HttpStatusCode.Accepted, response.StatusCode);

            var data = this.QueryCounter<HitCounter>(counterName);
            Assert.AreEqual(867 + 5309, data.HitCount);

            var dims = new DimensionSpecification();
            dims[dimensionName] = "0";
            data = this.QueryCounter<HitCounter>(counterName, dims);
            Assert.AreEqual(867, data.HitCount);

            dims[dimensionName] = "1";
            data = this.QueryCounter<HitCounter>(counterName, dims);
            Assert.AreEqual(5309, data.HitCount);
        }
    }
}
