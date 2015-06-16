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
    using System.Threading.Tasks;

    using MetricSystem.Data;
    using MetricSystem.Client;
    using MetricSystem.Client.UnitTests;
    using MetricSystem.Server.RequestHandlers;
    using MetricSystem.Utilities;

    using NUnit.Framework;

    using ServerInfo = MetricSystem.ServerInfo;

    [TestFixture]
    public sealed class BatchQueryHandlerTests : RequestHandlerTestBase
    {
        [Test]
        public void BatchQueryConstructorThrowsArgumentNullExceptionIfDataManagerIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new BatchQueryRequestHandler(null));
        }

        [Test]
        public async Task BatchQueryNonPostRequestFails()
        {
            var response = await
                this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.BatchQueryCommand, string.Empty));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task BatchQueryEmptyRequestBodyFails()
        {
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.BatchQueryCommand,
                                                                      string.Empty),
                                                     new ByteArrayContent(new byte[0]));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task BatchQueryNonEmptyQueryParametersFails()
        {
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.BatchQueryCommand,
                                                                      string.Empty,
                                                                      new[]
                                                                      {
                                                                          new KeyValuePair<string, string>(
                                                                              "anyKey", "anyValue"),
                                                                      }),
                                                     GetDefaultPayload());
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task BatchQueryNonEmptyUriPathFails()
        {
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.BatchQueryCommand,
                                                                      AnyCounter),
                                                     GetDefaultPayload());
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task BatchQueryGoodEnvelopeWithNoQueriesFails()
        {
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.BatchQueryCommand,
                                                                      string.Empty),
                                                     GetRequestPayload(new BatchQueryRequest()));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task BatchQueryMissingCounterStillProducesGoodResponse()
        {
            var data = new BatchQueryRequest();
            data.Queries.Add(new BatchCounterQuery {CounterName = "Tacos", UserContext = "Tacos"});

            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.BatchQueryCommand,
                                                                      string.Empty),
                                                     GetRequestPayload(data));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<BatchQueryResponse>();
                var responseData = reader.Deserialize();
                Assert.AreEqual(1, responseData.Responses.Count);

                var singleResponse =
                    (responseData.Responses as List<CounterQueryResponse>).Find(item => item.UserContext.Equals("Tacos"));
                Assert.IsNotNull(singleResponse);
                Assert.AreEqual((int)HttpStatusCode.NotFound, singleResponse.HttpResponseCode);
            }
        }

        [Test]
        public async Task BatchQueryMultipleQueriesForSameCounterWithDifferentParametersWorksFine()
        {
            // Fill up the taco truck
            var dimensionSet = new DimensionSet(new HashSet<Dimension> {new Dimension("Filling")});
            var counter = await this.dataManager.CreateHitCounter("/Tacos", dimensionSet);

            var chickenTacos = new DimensionSpecification {{"Filling", "Chicken"}};
            var beefTacos = new DimensionSpecification {{"Filling", "Beef"}};
            var veggieTacos = new DimensionSpecification {{"Filling", "TOFU"}};
            var baconTacos = new DimensionSpecification {{"Filling", "bacon"}};

            counter.Increment(100, chickenTacos);
            counter.Increment(200, beefTacos);
            counter.Increment(300, veggieTacos);
            this.dataManager.Flush();

            var data = new BatchQueryRequest();
            data.Queries.Add(new BatchCounterQuery {CounterName = "/Tacos", UserContext = "TotalTacos"});
            data.Queries.Add(new BatchCounterQuery
                             {
                                 CounterName = "/Tacos",
                                 UserContext = "CluckCluck",
                                 QueryParameters = chickenTacos.Data
                             });
            data.Queries.Add(new BatchCounterQuery
                             {
                                 CounterName = "/Tacos",
                                 UserContext = "BACON!",
                                 QueryParameters = baconTacos.Data
                             });

            var response = await
                           this.httpClient.PostAsync(
                                                     TestUtils.GetUri(this.server,
                                                                      RestCommands.BatchQueryCommand,
                                                                      string.Empty),
                                                     GetRequestPayload(data));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<BatchQueryResponse>();
                var responseData = reader.Deserialize();
                Assert.AreEqual(3, responseData.Responses.Count);

                // unfiltered should have 100+200+300 hitcount
                VerifyHitCounter(responseData, "TotalTacos", 600);

                //only 100 chicken tacos
                VerifyHitCounter(responseData, "CluckCluck", 100);

                //sadly, there is no bacon...
                VerifyHitCounter(responseData, "BACON!", -1);
            }
        }

        [Test]
        public async void BatchQueryAggregatesFanoutCorrectly()
        {
            const string tacoTruck = "/Tacos";
            const string competingBurritoTruck = "/Burritos";

            var dimensionSet = new DimensionSet(new HashSet<Dimension>());
            var counter = await this.dataManager.CreateHitCounter(tacoTruck, dimensionSet);

            // locally store dim1
            counter.Increment(100, new DimensionSpecification());
            this.dataManager.Flush();

            var data = new BatchQueryRequest();
            data.Queries.Add(new BatchCounterQuery { CounterName = tacoTruck, UserContext = tacoTruck});
            data.Queries.Add(new BatchCounterQuery
                             {
                                 CounterName = competingBurritoTruck,
                                 UserContext = competingBurritoTruck
                             });
            data.Sources.Add(new ServerInfo {Hostname = "a", Port = 42});
            data.Sources.Add(new ServerInfo {Hostname = "b", Port = 42});

            var sampleStart = DateTime.Now;
            var sampleEnd = sampleStart.AddHours(1);

            // remotely return 100 for dim2 only
            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(message =>
                    {
                        var batchResponse = new BatchQueryResponse();
                        batchResponse.RequestDetails.Add(new RequestDetails
                                                         {
                                                             Server =
                                                                 new ServerInfo
                                                                 {
                                                                     Hostname = "bob",
                                                                     Port = 42
                                                                 },
                                                             HttpResponseCode = 200
                                                         });
                        var counterResponse = new CounterQueryResponse
                                              {
                                                  HttpResponseCode = 200, 
                                                  UserContext = competingBurritoTruck,
                                                  Samples = new List<DataSample> {
                                                    new DataSample { HitCount = 100, Dimensions = new Dictionary<string, string>(), SampleType = DataSampleType.HitCount, StartTime = sampleStart.ToMillisecondTimestamp(), EndTime = sampleEnd.ToMillisecondTimestamp() }
                                                    }
                                               };
                        batchResponse.Responses.Add(counterResponse);
                        return MockDataFactory.CreateResponse(batchResponse);
                    });
            
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.BatchQueryCommand,
                                                                      string.Empty),
                                                     GetRequestPayload(data));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            var responseData = await MockDataFactory.ReadResponseData<BatchQueryResponse>(response);
            Assert.IsNotNull(responseData);
            Assert.AreEqual(3, responseData.RequestDetails.Count);
            Assert.IsTrue(responseData.RequestDetails.All(x => x.HttpResponseCode == 200));
            Assert.AreEqual(2, responseData.Responses.Count);
            Assert.AreEqual(1, responseData.Responses.Count(x => x.UserContext.Equals(tacoTruck) && x.Samples[0].HitCount == 100));
            Assert.AreEqual(1, responseData.Responses.Count(x => x.UserContext.Equals(competingBurritoTruck) && x.Samples[0].HitCount == 200));
        }

        private static void VerifyHitCounter(BatchQueryResponse responseData, string key, int expectedValue)
        {
            var queryResponse =
                (responseData.Responses as List<CounterQueryResponse>).Find(res => res.UserContext.Equals(key));
            Assert.IsNotNull(queryResponse);

            if (expectedValue > 0)
            {
                Assert.IsTrue(queryResponse.HttpResponseCode == 200);
                Assert.IsNotNull(queryResponse);
                Assert.AreEqual(1, queryResponse.Samples.Count);
                var hitCountSample = queryResponse.Samples[0];
                Assert.IsTrue(expectedValue == (int)hitCountSample.HitCount);
            }
            else
            {
                Assert.IsFalse(queryResponse.HttpResponseCode == 200);
            }
        }

        private static ByteArrayContent GetDefaultPayload()
        {
            return GetRequestPayload(new BatchQueryRequest
                                     {
                                         Queries = new List<BatchCounterQuery>
                                                   {
                                                       new BatchCounterQuery
                                                       {
                                                           CounterName = AnyCounter,
                                                           QueryParameters = new Dictionary<string, string>(),
                                                           UserContext = string.Empty,
                                                       }
                                                   }
                                     });
        }

        private static ByteArrayContent GetRequestPayload(BatchQueryRequest data)
        {
            using (var ms = new MemoryStream())
            using (var writerStream = new WriterStream(ms))
            {
                writerStream.CreateCompactBinaryWriter().Write(data);
                return new ByteArrayContent(ms.GetBuffer());
            }
        }
    }
}
