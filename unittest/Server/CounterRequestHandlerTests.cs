// ---------------------------------------------------------------------
// <copyright file="CounterRequestHandlerTests.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Server.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;

    using MetricSystem.Data;
    using MetricSystem.Client;
    using MetricSystem.Client.UnitTests;
    using MetricSystem.Server.RequestHandlers;
    using MetricSystem.Utilities;

    using NUnit.Framework;

    using ServerInfo = MetricSystem.ServerInfo;

    [TestFixture]
    public sealed class CountersRequestHandlerTests
    {
        internal const string AnyCounter = "/Any Counter";

        private static readonly MetricSystem.ServerInfo AnyRemoteSource = new MetricSystem.ServerInfo
                                                                          {
                                                                              Hostname = "TacoBell",
                                                                              Port = 42
                                                                          };
        internal DataManager dataManager;
        internal CountersRequestHandler handler;
        private HttpClient httpClient;
        private Server server;

        [SetUp]
        public void SetUp()
        {
            this.dataManager = new DataManager();
            this.server = new Server("localhost", 0, this.dataManager);
            this.server.Start();
            this.httpClient = new HttpClient(
                new WebRequestHandler
                {
                    AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip
                });
            this.httpClient.DefaultRequestHeaders.Add("Accept", Protocol.BondCompactBinaryMimeType);
        }

        [TearDown]
        public void TearDown()
        {
            this.server.Dispose();
            this.httpClient.Dispose();
        }

        [Test]
        public void ConstructorThrowsArgumentNullExceptionIfDataManagerIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new CountersRequestHandler(null));
        }

        [Test]
        public async Task ErrorResponseIsSentIfUnknownCounterIsRequestedForQuery()
        {
            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     AnyCounter, RestCommands.CounterQueryCommand));
            Assert.AreEqual(HttpStatusCode.NotFound, response.StatusCode);
        }

        [Test]
        public async Task OKResponseIsSentIfDataIsFoundForQuery()
        {
            var counter =
                await this.dataManager.CreateHitCounter(AnyCounter, new DimensionSet(new HashSet<Dimension>()));
            counter.Increment(new DimensionSpecification());
            this.dataManager.Flush();

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     AnyCounter, RestCommands.CounterQueryCommand));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        }

        [Test]
        public async Task ResponseForQueryIsBondSerializedQueryDataQueryResponse()
        {
            var counter =
                await this.dataManager.CreateHitCounter(AnyCounter, new DimensionSet(new HashSet<Dimension>()));
            counter.Increment(2, new DimensionSpecification());
            this.dataManager.Flush();

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     AnyCounter, RestCommands.CounterQueryCommand));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterQueryResponse>();
                var bondResp = reader.Deserialize();
                Assert.AreEqual(1, bondResp.Samples.Count);
                var sample = bondResp.Samples[0];
                Assert.AreEqual((ulong)2, sample.HitCount);
            }
        }

        [Test]
        public async Task ErrorResponseIsSentIfNoDimensionsMatchForQuery()
        {
            const string anyDimensionValue = "dv";
            var anyDimensionSet = new DimensionSet(new HashSet<Dimension> {new Dimension("foo")});
            var anyDimensions = new DimensionSpecification();
            foreach (var k in anyDimensionSet.Dimensions)
            {
                anyDimensions[k.Name] = anyDimensionValue;
            }

            var counter = await this.dataManager.CreateHitCounter(AnyCounter, anyDimensionSet);
            counter.Increment(anyDimensions);
            this.dataManager.Flush();

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     AnyCounter, RestCommands.CounterQueryCommand,
                                                                     new Dictionary<string, string>
                                                                     {
                                                                         {
                                                                             anyDimensionSet.Dimensions.First().Name,
                                                                             anyDimensionValue + "baz"
                                                                         }
                                                                     }));
            Assert.AreEqual(HttpStatusCode.NotFound, response.StatusCode);
        }

        [Test]
        public async Task OKResponseIsSentIfDimensionsMatchForQuery()
        {
            const string anyDimensionValue = "dv";
            var anyDimensionSet = new DimensionSet(new HashSet<Dimension> {new Dimension("foo")});
            var anyDimensions = new DimensionSpecification();
            foreach (var k in anyDimensionSet.Dimensions)
            {
                anyDimensions[k.Name] = anyDimensionValue;
            }

            var counter = await this.dataManager.CreateHitCounter(AnyCounter, anyDimensionSet);
            counter.Increment(anyDimensions);
            this.dataManager.Flush();

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     AnyCounter, RestCommands.CounterQueryCommand,
                                                                     new Dictionary<string, string>
                                                                     {
                                                                         {
                                                                             anyDimensionSet.Dimensions.First().Name,
                                                                             anyDimensionValue
                                                                         }
                                                                     }));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        }

        [Test]
        public async Task OnlyFilteredDimensionsAreSentForQueryResponseData()
        {
            const string anyDimensionValue = "dv";
            var anyDimensionSet = new DimensionSet(new HashSet<Dimension> {new Dimension("foo")});
            var anyDimensions = new DimensionSpecification();
            foreach (var k in anyDimensionSet.Dimensions)
            {
                anyDimensions[k.Name] = anyDimensionValue;
            }

            var counter = await this.dataManager.CreateHitCounter(AnyCounter, anyDimensionSet);
            counter.Increment(2, anyDimensions);
            anyDimensions[anyDimensionSet.Dimensions.First().Name] = anyDimensionValue + "bar";
            counter.Increment(anyDimensions);
            this.dataManager.Flush();

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     AnyCounter, RestCommands.CounterQueryCommand,
                                                                     new Dictionary<string, string>
                                                                     {
                                                                         {
                                                                             anyDimensionSet.Dimensions.First().Name,
                                                                             anyDimensionValue
                                                                         }
                                                                     }));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterQueryResponse>();
                var bondResp = reader.Deserialize();
                Assert.AreEqual(1, bondResp.Samples.Count);
                var sample = bondResp.Samples[0];
                Assert.AreEqual((ulong)2, sample.HitCount);

                // Same request but with no params should yield 3.
                response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server,
                                                                     RestCommands.CounterRequestCommand,
                                                                     AnyCounter, RestCommands.CounterQueryCommand));
                Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

                using (var secondReaderStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
                {
                    reader = secondReaderStream.CreateBondedCompactBinaryReader<CounterQueryResponse>();
                    bondResp = reader.Deserialize();
                    Assert.AreEqual(1, bondResp.Samples.Count);
                    sample = bondResp.Samples[0];
                    Assert.AreEqual((ulong)3, sample.HitCount);
                }
            }
        }

        [Test]
        public async Task SplitByQueryReturnsAllData()
        {
            var dimensionSet =
                new DimensionSet(new HashSet<Dimension> {new Dimension("Anything"), new Dimension("OptionalFilter")});
            var counter = await this.dataManager.CreateHitCounter(AnyCounter, dimensionSet);

            var dim1 = new DimensionSpecification {{"Anything", "abc"}, {"OptionalFilter", "123"}};
            counter.Increment(dim1);

            var dim2 = new DimensionSpecification {{"Anything", "def"}, {"OptionalFilter", "456"}};
            counter.Increment(dim2);

            var dim3 = new DimensionSpecification {{"Anything", "ghi"}, {"OptionalFilter", "789"}};
            counter.Increment(dim3);

            this.dataManager.Flush();

            var queryDimensions = new Dictionary<string, string>
                                  {
                                      {"dimension", "Anything"},
                                      {"aggregate", "true"},
                                      {"percentile", "average"}
                                  };

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     AnyCounter, RestCommands.CounterQueryCommand,
                                                                     queryDimensions));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateCompactBinaryReader();
                var responseData = reader.Read<CounterQueryResponse>();
                Assert.IsNotNull(responseData);
                Assert.AreEqual(3, responseData.Samples.Count);

                // split by, but filter to one expected sample
                queryDimensions.Add("OptionalFilter", "456");
                response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server,
                                                                     RestCommands.CounterRequestCommand,
                                                                     AnyCounter, RestCommands.CounterQueryCommand,
                                                                     queryDimensions));
                Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
                using (var secondReaderStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
                {
                    reader = secondReaderStream.CreateCompactBinaryReader();
                    responseData = reader.Read<CounterQueryResponse>();
                    Assert.IsNotNull(responseData);
                    Assert.AreEqual(1, responseData.Samples.Count);
                }
            }
        }

        [Test]
        public async Task InfoReturnsBadRequestErrorIfNoPatternIsProvided()
        {
            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     "/" + RestCommands.CounterInfoCommand));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task InfoReturnsNotFoundErrorIfNoCountersAreDefined()
        {
            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     "/*", RestCommands.CounterInfoCommand));
            Assert.AreEqual(HttpStatusCode.NotFound, response.StatusCode);
        }

        [Test]
        public async Task InfoReturnsAllCountersIfWildcardPatternIsProvided()
        {
            const string anyCounter2 = AnyCounter + "/2";
            await this.dataManager.CreateHitCounter(AnyCounter, DimensionSet.Empty);
            await this.dataManager.CreateHitCounter(anyCounter2, DimensionSet.Empty);

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     "/*", RestCommands.CounterInfoCommand));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterInfoResponse>();
                var bondResp = reader.Deserialize();
                Assert.AreEqual(2, bondResp.Counters.Count);
                var c1 = bondResp.Counters.FirstOrDefault(c => c.Name.Equals(AnyCounter));
                Assert.IsNotNull(c1);
                Assert.AreEqual(CounterType.HitCount, c1.Type);
                var c2 = bondResp.Counters.FirstOrDefault(c => c.Name.Equals(anyCounter2));
                Assert.IsNotNull(c2);
                Assert.AreEqual(CounterType.HitCount, c2.Type);
            }
        }

        [Test]
        public async Task InfoReturnsOnlyMatchingCountersForWildcard()
        {
            const string anyCounter2 = AnyCounter + "/foo2";
            const string anyCounter3 = AnyCounter + "/foo3";
            await this.dataManager.CreateHitCounter(AnyCounter, DimensionSet.Empty);
            await this.dataManager.CreateHitCounter(anyCounter2, DimensionSet.Empty);
            await this.dataManager.CreateHitCounter(anyCounter3, DimensionSet.Empty);

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     AnyCounter + "/foo*",
                                                                     RestCommands.CounterInfoCommand));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterInfoResponse>();
                var bondResp = reader.Deserialize();
                Assert.AreEqual(2, bondResp.Counters.Count);
                var c2 = bondResp.Counters.FirstOrDefault(c => c.Name.Equals(anyCounter2));
                Assert.IsNotNull(c2);
                Assert.AreEqual(CounterType.HitCount, c2.Type);
                var c3 = bondResp.Counters.FirstOrDefault(c => c.Name.Equals(anyCounter3));
                Assert.IsNotNull(c3);
                Assert.AreEqual(CounterType.HitCount, c3.Type);
            }
        }

        [Test]
        public async Task DetailsVerifiedPOSTBody()
        {
            await this.dataManager.CreateHitCounter(AnyCounter, new DimensionSet(new HashSet<Dimension>()));
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                      AnyCounter, RestCommands.CounterInfoCommand),
                                                     new ByteArrayContent(Encoding.UTF8.GetBytes("I am a key tree!")));
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task InfoSupportsFanout()
        {
            var d1 = new Dimension("d1");
            var d2 = new Dimension("d2");
            const string val1 = "tacos";
            const string val2 = "other";

            const string counter1 = "/C1";
            const string counter2 = "/C2";

            var counter =
                await this.dataManager.CreateHitCounter(counter1, new DimensionSet(new HashSet<Dimension> {d1}));
            counter.Increment(1, new DimensionSpecification(new Dictionary<string, string> {{d1.Name, val1}}));
            counter.DataSet.Flush();

            var mockResponse =
                new CounterInfoResponse
                {
                    Counters = new List<CounterInfo>
                               {
                                   // Here we duplicate 'val1' but in uppercase to ensure merging is case-insensitive.
                                   new CounterInfo
                                   {
                                       Name = counter1,
                                       Type = CounterType.HitCount,
                                       Dimensions = new List<string>(new[] {d2.Name}),
                                       DimensionValues = new Dictionary<string, ISet<string>>
                                                         {
                                                             {d1.Name, new HashSet<string> {val1.ToUpper(), val2}},
                                                             {d2.Name, new HashSet<string>()}
                                                         }
                                   },
                                   new CounterInfo
                                   {
                                       Name = counter2,
                                       Type = CounterType.HitCount,
                                       Dimensions = new List<string>(new[] {d2.Name}),
                                       DimensionValues = new Dictionary<string, ISet<string>>
                                                         {{d2.Name, new HashSet<string> {val2}}}
                                   }
                               }
                };
            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(
                _ => MockDataFactory.CreateResponse(mockResponse));

            var queryParams = new Dictionary<string, string> {{ReservedDimensions.DimensionDimension, "*"}};
            var response = await
                           this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                      "/*", RestCommands.CounterInfoCommand, queryParams),
                                                     GetQueryPayload(CreateFanoutRequest(2)));

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            var infoResponse = await MockDataFactory.ReadResponseData<CounterInfoResponse>(response);
            Assert.IsNotNull(infoResponse);
            Assert.AreEqual(2, infoResponse.Counters.Count);
            var c1 = infoResponse.Counters.First(c => c.Name.Equals(counter1));
            var c2 = infoResponse.Counters.First(c => c.Name.Equals(counter2));
            Assert.AreEqual(2, c1.Dimensions.Count);
            Assert.IsTrue(c1.Dimensions.Contains(d1.Name));
            Assert.IsTrue(c1.Dimensions.Contains(d2.Name));
            Assert.IsTrue(c1.DimensionValues.ContainsKey(d1.Name));
            Assert.AreEqual(2, c1.DimensionValues[d1.Name].Count);
            // Prior to calling 'contains' below we want to ensure case-insensitivity (it's potentially necessary),
            // however we want to do this later than checking the total count directly above to ensure that
            // we only got two values back.
            c1.FixDimensionValuesCaseSensitivity();
            Assert.IsTrue(c1.DimensionValues[d1.Name].Contains(val1));
            Assert.IsTrue(c1.DimensionValues[d1.Name].Contains(val2));
            Assert.IsTrue(c1.DimensionValues.ContainsKey(d2.Name));
            Assert.AreEqual(0, c1.DimensionValues[d2.Name].Count);

            Assert.AreEqual(1, c2.Dimensions.Count);
            Assert.IsTrue(c2.Dimensions.Contains(d2.Name));
            Assert.IsTrue(c2.DimensionValues.ContainsKey(d2.Name));
            Assert.AreEqual(1, c2.DimensionValues[d2.Name].Count);
            c2.FixDimensionValuesCaseSensitivity();
            Assert.IsTrue(c2.DimensionValues[d2.Name].Contains(val2));
        }

        [Test]
        public async Task InfoReturnsEmptyListIfDataDoesNotHaveDimensions()
        {
            await this.dataManager.CreateHitCounter(AnyCounter, new DimensionSet(new HashSet<Dimension>()));

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server,
                                                                     RestCommands.CounterRequestCommand,
                                                                     AnyCounter,
                                                                     RestCommands.CounterInfoCommand));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterInfoResponse>();
                var bondResp = reader.Deserialize();
                Assert.AreEqual(AnyCounter, bondResp.Counters[0].Name);
                Assert.AreEqual(0, bondResp.Counters[0].Dimensions.Count);
            }
        }

        [Test]
        public async Task InfoReturnsAllDimensionsForDataInJsonFormat()
        {
            await this.InfoReturnsAllDimensionsForDataAndEncodesInCorrectFormat(false);
        }

        [Test]
        public async Task InfoReturnsAllDimensionsForDataInCompactBinaryFormatFormat()
        {
            await this.InfoReturnsAllDimensionsForDataAndEncodesInCorrectFormat(true);
        }

        private async Task InfoReturnsAllDimensionsForDataAndEncodesInCorrectFormat(bool isCompactBinaryEncoding)
        {
            var anyDimensionSet =
                new DimensionSet(new HashSet<Dimension> {new Dimension("d1"), new Dimension("d2"), new Dimension("d3")});
            await this.dataManager.CreateHitCounter(AnyCounter, anyDimensionSet);

            var request = new HttpRequestMessage(HttpMethod.Get,
                                                 TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                  AnyCounter,
                                                                  RestCommands.CounterInfoCommand));
            if (isCompactBinaryEncoding)
            {
                request.Headers.Add("Accept", Protocol.BondCompactBinaryMimeType);
            }
            else
            {
                request.Headers.Add("Accept", Protocol.ApplicationJsonMimeType);
            }
            var response = await this.httpClient.SendAsync(request);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            var bondResp = new CounterInfoResponse();
            if (isCompactBinaryEncoding)
            {
                using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
                {
                    var reader = readerStream.CreateBondedCompactBinaryReader<CounterInfoResponse>();
                    bondResp = reader.Deserialize();
                }
            }
            else
            {
                // Hacky validation
                var respStr = Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync());
                Assert.IsTrue(respStr.Contains('{'));
                return;
            }

            Assert.AreEqual(AnyCounter, bondResp.Counters[0].Name);
            Assert.AreEqual(anyDimensionSet.Dimensions.Count(), bondResp.Counters[0].Dimensions.Count);
            foreach (var d in anyDimensionSet.Dimensions)
            {
                Assert.IsTrue(bondResp.Counters[0].Dimensions.Contains(d.Name));
            }
        }

        [Test]
        public async Task InfoWithDimensionParameterReturnsEmptyListIfDimensionHasNoValues()
        {
            const string anyDimension = "d";
            await this.dataManager.CreateHitCounter(AnyCounter,
                                                    new DimensionSet(new HashSet<Dimension>
                                                                     {
                                                                         new Dimension(anyDimension)
                                                                     }));

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     AnyCounter,
                                                                     RestCommands.CounterInfoCommand,
                                                                     new Dictionary<string, string>
                                                                     {
                                                                         {
                                                                             ReservedDimensions.DimensionDimension,
                                                                             anyDimension
                                                                         }
                                                                     }));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterInfoResponse>();
                var bondResp = reader.Deserialize();
                var counterInfo = bondResp.Counters.First(c => c.Name.Equals(AnyCounter));
                Assert.AreEqual(AnyCounter, counterInfo.Name);
                Assert.IsTrue(counterInfo.DimensionValues.ContainsKey(anyDimension));
            }
        }

        [Test]
        public async Task InfoWithDimensionParameterReturnsAllDimensionValuesForDimension()
        {
            const string anyDimension = "d";
            var counter = await this.dataManager.CreateHitCounter(AnyCounter,
                                                                  new DimensionSet(new HashSet<Dimension>
                                                                                   {
                                                                                       new Dimension
                                                                                           (anyDimension)
                                                                                   }));
            var anyDimensionValues = new HashSet<string> {"dv1", "dv2", "dv3"};
            foreach (var dv in anyDimensionValues)
            {
                counter.Increment(new DimensionSpecification {{anyDimension, dv}});
            }

            this.dataManager.Flush();

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     AnyCounter,
                                                                     RestCommands.CounterInfoCommand,
                                                                     new Dictionary<string, string>
                                                                     {
                                                                         {
                                                                             ReservedDimensions.DimensionDimension,
                                                                             anyDimension
                                                                         }
                                                                     }));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterInfoResponse>();
                var bondResp = reader.Deserialize();
                var counterInfo = bondResp.Counters.First(c => c.Name.Equals(AnyCounter));
                Assert.AreEqual(AnyCounter, counterInfo.Name);
                Assert.IsTrue(counterInfo.DimensionValues.ContainsKey(anyDimension));
                Assert.AreEqual(anyDimensionValues.Count, counterInfo.DimensionValues[anyDimension].Count);
                foreach (var dv in anyDimensionValues)
                {
                    Assert.IsTrue(counterInfo.DimensionValues[anyDimension].Contains(dv));
                }
            }
        }

        [Test]
        public async Task AggregateSamplesCombinesResponseIntoSingleSample()
        {
            var timestamp = DateTime.Now;
            var pastTimestamp = timestamp.AddMinutes(-6);

            var counter =
                await this.dataManager.CreateHitCounter(AnyCounter, new DimensionSet(new HashSet<Dimension>()));
            counter.Increment(2, new DimensionSpecification(), pastTimestamp);
            counter.Increment(2, new DimensionSpecification(), timestamp);
            this.dataManager.Flush();

            var response = await
                           this.httpClient.GetAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                     AnyCounter, RestCommands.CounterQueryCommand,
                                                                     new Dictionary<string, string>
                                                                     {{"aggregate", "true"}}));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterQueryResponse>();
                var bondResp = reader.Deserialize();
                Assert.AreEqual(1, bondResp.Samples.Count);
                var sample = bondResp.Samples[0];
                Assert.AreEqual((ulong)4, sample.HitCount);
            }
        }

        [Test]
        public async Task CounterRequestHandlerAcceptsEmptySources()
        {
            var counter =
                await this.dataManager.CreateHitCounter(AnyCounter, new DimensionSet(new HashSet<Dimension>()));
            counter.Increment(2, new DimensionSpecification());
            this.dataManager.Flush();

            // no sources - should return 200 with proper data
            var response = await
                           this.httpClient.PostAsync(
                                                     TestUtils.GetUri(this.server,
                                                                      RestCommands.CounterRequestCommand,
                                                                      AnyCounter, RestCommands.CounterQueryCommand),
                                                     GetQueryPayload(new TieredRequest()));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterQueryResponse>();
                var payload = reader.Deserialize();
                Assert.IsTrue(payload.Samples.Count > 0);
                Assert.AreEqual(1, payload.RequestDetails.Count);
            }
        }

        [Test]
        public async Task CounterRequestHandlerObeysDiagnosticsFlag()
        {
            var counter =
                await this.dataManager.CreateHitCounter(AnyCounter, new DimensionSet(new HashSet<Dimension>()));
            counter.Increment(2, new DimensionSpecification());
            this.dataManager.Flush();

            // no sources - should return 200 with proper data
            var response = await
                           this.httpClient.PostAsync(
                                                     TestUtils.GetUri(this.server,
                                                                      RestCommands.CounterRequestCommand,
                                                                      AnyCounter, RestCommands.CounterQueryCommand),
                                                     GetQueryPayload(new TieredRequest
                                                                     {
                                                                         IncludeRequestDiagnostics
                                                                             = false
                                                                     }));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterQueryResponse>();
                var payload = reader.Deserialize();
                Assert.IsTrue(payload.Samples.Count > 0);
                Assert.IsTrue(payload.RequestDetails == null || payload.RequestDetails.Count == 0);
            }
        }

        [Test]
        public async Task CounterRequestHandlerHandlesGoodLocalDataAndFailingDistributedResponses()
        {
            var counter =
                await this.dataManager.CreateHitCounter(AnyCounter, new DimensionSet(new HashSet<Dimension>()));
            counter.Increment(2, new DimensionSpecification());
            this.dataManager.Flush();

            // distributed request...it shall fail!
            DistributedQueryClient.RequesterFactory =
                new MockHttpRequesterFactory(_ => { throw new IOException("Hehehe"); });

            var response =
                await this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                 AnyCounter, RestCommands.CounterQueryCommand),
                                                GetQueryPayload(new TieredRequest
                                                                {
                                                                    Sources =
                                                                        new List<MetricSystem.ServerInfo>
                                                                        {
                                                                            AnyRemoteSource,
                                                                        }
                                                                }));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterQueryResponse>();
                var payload = reader.Deserialize();
                Assert.IsTrue(payload.Samples.Count > 0);
                Assert.AreEqual(2, payload.RequestDetails.Count);
                Assert.AreEqual(1,
                                payload.RequestDetails.Count(
                                                             i =>
                                                             i.Server.Equals(AnyRemoteSource) &&
                                                             i.Status == RequestStatus.RequestException));
            }
        }

        [Test]
        public async Task CounterRequestHandlerHandlesFailingLocalQueryAndSucceedingDistributedResponse()
        {
            // add a counter that has a specific dimension. 
            var counter =
                await this.dataManager.CreateHitCounter(AnyCounter,
                                                        new DimensionSet(new HashSet<Dimension> {new Dimension("food")}));
            counter.Increment(2, new DimensionSpecification {{"food", "delicious"}});
            this.dataManager.Flush();

            // distributed request...it shall succeed
            DistributedQueryClient.RequesterFactory =
                new MockHttpRequesterFactory(
                    request => MockDataFactory.CreateGoodHitCountResponse(request, DateTime.Now, 1).Result);

            // query where the local results will fail (404)
            var queryParameters = new Dictionary<string, string> {{"food", "gross"}};
            var response =
                await this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                 AnyCounter, RestCommands.CounterQueryCommand,
                                                                 queryParameters),
                                                GetQueryPayload(new TieredRequest
                                                                {
                                                                    Sources = new List<MetricSystem.ServerInfo> {AnyRemoteSource}
                                                                }));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterQueryResponse>();
                var payload = reader.Deserialize();
                Assert.IsTrue(payload.Samples.Count > 0);
                Assert.AreEqual(2, payload.RequestDetails.Count);

                // remote should succeed
                Assert.AreEqual(1,
                                payload.RequestDetails.Count(
                                                             i =>
                                                             i.Server.Equals(AnyRemoteSource) &&
                                                             i.Status == RequestStatus.Success));

                // local should fail with 404
                Assert.AreEqual(1,
                                payload.RequestDetails.Count(
                                                             i =>
                                                             i.Status == RequestStatus.ServerFailureResponse &&
                                                             i.HttpResponseCode == 404));
            }
        }

        [Test]
        public async Task CounterRequestHandlerHandles400LocalQueryAndSucceedingDistributedResponse()
        {
            // add a counter that has a specific dimension. 
            var counter =
                await this.dataManager.CreateHitCounter(AnyCounter,
                                                        new DimensionSet(new HashSet<Dimension> {new Dimension("food")}));
            counter.Increment(2, new DimensionSpecification {{"food", "delicious"}});
            this.dataManager.Flush();

            // distributed request...it shall succeed
            DistributedQueryClient.RequesterFactory =
                new MockHttpRequesterFactory(
                    request => MockDataFactory.CreateGoodHitCountResponse(request, DateTime.Now, 1).Result);

            // query where the local results will fail (404)
            var queryParameters = new Dictionary<string, string> {{"start", "i am a key tree...exception!"}};

            var response =
                await this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                 AnyCounter, RestCommands.CounterQueryCommand,
                                                                 queryParameters),
                                                GetQueryPayload(new TieredRequest
                                                                {
                                                                    Sources = new List<MetricSystem.ServerInfo> {AnyRemoteSource}
                                                                }));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterQueryResponse>();
                var payload = reader.Deserialize();
                Assert.IsTrue(payload.Samples.Count > 0);
                Assert.AreEqual(2, payload.RequestDetails.Count);

                // remote should succeed
                Assert.AreEqual(1,
                                payload.RequestDetails.Count(
                                                             i =>
                                                             i.Server.Equals(AnyRemoteSource) &&
                                                             i.Status == RequestStatus.Success));

                // local should fail with 404
                Assert.AreEqual(1,
                                payload.RequestDetails.Count(
                                                             i =>
                                                             i.Status == RequestStatus.ServerFailureResponse &&
                                                             i.HttpResponseCode == 400));
            }
        }

        [Test]
        public async Task CounterRequestHandlerReturns404IfNoDataInTimeRange()
        {
            // add a counter that has a specific dimension. 
            var counter =
                await this.dataManager.CreateHitCounter(AnyCounter,
                                                        new DimensionSet(new HashSet<Dimension> {new Dimension("food")}));
            counter.Increment(2, new DimensionSpecification {{"food", "delicious"}});
            this.dataManager.Flush();

            // distributed request...it shall succeed
            DistributedQueryClient.RequesterFactory =
                new MockHttpRequesterFactory(
                    request => MockDataFactory.CreateGoodHitCountResponse(request, DateTime.Now, 1).Result);

            // query where the local results will fail (404)
            var queryParameters = new Dictionary<string, string> {{"start", DateTime.Now.AddDays(7).ToString("o")}};
            var response =
                await this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                 AnyCounter, RestCommands.CounterQueryCommand,
                                                                 queryParameters),
                                                GetQueryPayload(new TieredRequest()));
            Assert.AreEqual(HttpStatusCode.NotFound, response.StatusCode);
        }

        [Test]
        public async Task CounterRequestHandlerMergesResponsesProperly()
        {
            // add a counter that has a specific dimension. 
            var counter =
                await this.dataManager.CreateHitCounter(AnyCounter, new DimensionSet(new HashSet<Dimension>()));
            counter.Increment(1, new DimensionSpecification());
            this.dataManager.Flush();

            // distributed request...it shall succeed
            DistributedQueryClient.RequesterFactory =
                new MockHttpRequesterFactory(
                    request => MockDataFactory.CreateGoodHitCountResponse(request, DateTime.Now, 1).Result);

            // query where the local results will fail (404)
            var response =
                await this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                 AnyCounter, RestCommands.CounterQueryCommand),
                                                GetQueryPayload(new TieredRequest
                                                                {
                                                                    Sources = new List<MetricSystem.ServerInfo> {AnyRemoteSource}
                                                                }));
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterQueryResponse>();
                var payload = reader.Deserialize();
                Assert.IsTrue(payload.Samples.Count > 0);

                // should have merged local + remote to 2 hits
                Assert.IsTrue(payload.Samples[0].HitCount == 2);
                Assert.AreEqual(2, payload.RequestDetails.Count);
                Assert.AreEqual(2, payload.RequestDetails.Count(i => i.Status == RequestStatus.Success));
            }
        }

        [Test]
        public async Task CounterRequestHandlerGeneratesConsistentErrorCode()
        {
            // add a counter that has a specific dimension. 
            var counter =
                await this.dataManager.CreateHitCounter(AnyCounter,
                                                        new DimensionSet(new HashSet<Dimension> {new Dimension("food")}));
            counter.Increment(2, new DimensionSpecification {{"food", "delicious"}});
            this.dataManager.Flush();

            // 404 distributed
            DistributedQueryClient.RequesterFactory =
                new MockHttpRequesterFactory(
                    request => MockDataFactory.CreateFailedTieredResponse("anyhost", HttpStatusCode.NotFound));

            // query where the local results will fail (404)
            var queryParameters = new Dictionary<string, string> {{"food", "gross"}};
            var response =
                await this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                 AnyCounter, RestCommands.CounterQueryCommand,
                                                                 queryParameters),
                                                GetQueryPayload(new TieredRequest
                                                                {
                                                                    Sources = new List<MetricSystem.ServerInfo> {AnyRemoteSource}
                                                                }));

            // should get a 404
            Assert.AreEqual(HttpStatusCode.NotFound, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterQueryResponse>();
                var payload = reader.Deserialize();

                // no samples, but 1 diagnostic from the remote server
                Assert.AreEqual(0, payload.Samples.Count);
                Assert.AreEqual(2, payload.RequestDetails.Count);
            }
        }

        [Test]
        public async Task CounterRequestHandlerGeneratesMergedErrorCode()
        {
            // add a counter that has a specific dimension. 
            var counter =
                await this.dataManager.CreateHitCounter(AnyCounter,
                                                        new DimensionSet(new HashSet<Dimension> {new Dimension("food")}));
            counter.Increment(2, new DimensionSpecification {{"food", "delicious"}});
            this.dataManager.Flush();

            // 404 distributed
            DistributedQueryClient.RequesterFactory =
                new MockHttpRequesterFactory(
                    request => MockDataFactory.CreateFailedTieredResponse("anyhost", HttpStatusCode.PaymentRequired));

            // query where the local results will fail (404)
            var queryParameters = new Dictionary<string, string> {{"food", "gross"}};
            var response =
                await this.httpClient.PostAsync(TestUtils.GetUri(this.server, RestCommands.CounterRequestCommand,
                                                                 AnyCounter, RestCommands.CounterQueryCommand,
                                                                 queryParameters),
                                                GetQueryPayload(new TieredRequest
                                                                {
                                                                    Sources = new List<MetricSystem.ServerInfo> {AnyRemoteSource}
                                                                }));

            // should get a 404
            Assert.AreEqual(HttpStatusCode.Conflict, response.StatusCode);
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                var reader = readerStream.CreateBondedCompactBinaryReader<CounterQueryResponse>();
                var payload = reader.Deserialize();

                // no samples, but diagnostics from both machines
                Assert.AreEqual(0, payload.Samples.Count);
                Assert.AreEqual(2, payload.RequestDetails.Count);
            }
        }

        private static TieredRequest CreateFanoutRequest(int sources)
        {
            var request = new TieredRequest();

            for (int i = 0; i < sources; i++)
            {
                request.Sources.Add(new ServerInfo {Hostname = i.ToString(), Port = 42});
            }

            return request;
        }

        private static ByteArrayContent GetQueryPayload<TData>(TData data)
        {
            using (var ms = new MemoryStream())
            {
                using (var writerStream = new WriterStream(ms))
                {
                    var writer = writerStream.CreateCompactBinaryWriter();
                    writer.Write(data);
                    return new ByteArrayContent(ms.GetBuffer());
                }
            }
        }
    }
}
