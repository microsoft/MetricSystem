namespace MetricSystem.Client.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;

    using NUnit.Framework;

    [TestFixture]
    internal class DistributedQueryClientTests
    {
        private readonly static TimeSpan DefaultTimeOut = TimeSpan.FromTicks(8675309);
        private readonly DistributedQueryClient client = new DistributedQueryClient(DefaultTimeOut);

        [Test]
        public void ClientVerifiesArguments()
        {
            Assert.Throws<ArgumentException>(async () =>
                                             await this.client.CounterQuery(null, new TieredRequest()));
            Assert.Throws<ArgumentException>(async () =>
                                             await this.client.CounterQuery(string.Empty, new TieredRequest()));
            Assert.Throws<ArgumentNullException>(async () =>
                                                 await this.client.CounterQuery("/Tacos", (ServerInfo)null));
            Assert.Throws<ArgumentNullException>(async () =>
                                                 await this.client.CounterQuery("/Tacos", (TieredRequest)null));

            Assert.Throws<ArgumentNullException>(async () => await this.client.BatchQuery(null));

            Assert.Throws<ArgumentException>(async () =>
                                             await this.client.CounterInfoQuery(null, new TieredRequest()));
            Assert.Throws<ArgumentException>(async () =>
                                             await this.client.CounterInfoQuery(string.Empty, new TieredRequest()));
            Assert.Throws<ArgumentNullException>(async () =>
                                                 await this.client.CounterInfoQuery("/Tacos", (ServerInfo)null));
            Assert.Throws<ArgumentNullException>(async () =>
                                                 await this.client.CounterInfoQuery("/Tacos", (TieredRequest)null));
        }

        [Test]
        public async void ClientGeneratesCorrectQueryUri()
        {
            const string expectedUri = "http://a:1/counters/AnyCounter/query";
            var passed = false;

            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(requestMessage =>
                    {
                        passed = requestMessage.RequestUri.ToString().Equals(expectedUri);
                        throw new WebException();
                    });

            var request = new TieredRequest
                          {
                              FanoutTimeoutInMilliseconds = 10,
                              Sources = new List<ServerInfo>(new[] {new ServerInfo { Hostname = "a", Port = 1} }),
                              MaxFanout = 2
                          };

            var response = await this.client.CounterQuery("/AnyCounter", request, null);
            Assert.IsNotNull(response);
            Assert.IsTrue(passed);
        }

        [Test]
        public async void ClientGeneratesCorrectDetailsUri()
        {
            const string expectedUri = "http://a:1/counters/AnyCounter/info?dimension=tacos";
            var passed = false;

            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(requestMessage =>
            {
                passed = requestMessage.RequestUri.ToString().Equals(expectedUri);
                throw new WebException();
            });


            var sources = new List<ServerInfo>(new[] { new ServerInfo { Hostname = "a", Port = 1 } });
            var response =
                await
                this.client.CounterInfoQuery("/AnyCounter", new TieredRequest { Sources = sources, },
                                             new Dictionary<string, string>
                                             {
                                                 {
                                                     ReservedDimensions.DimensionDimension,
                                                     "tacos"
                                                 }
                                             });
            Assert.IsNotNull(response);
            Assert.IsTrue(passed);
        }

        [Test]
        public async void ClientRequestsDataFromEachMachineExactlyOnce()
        {
            var request = new TieredRequest
            {
                FanoutTimeoutInMilliseconds = 10,
                MaxFanout = 2
            };

            var allMachines = new List<ServerInfo>();
            foreach (var name in new[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"})
            {
                allMachines.Add(new ServerInfo {Hostname = name, Port = 42});
            }
            var seenMachines = new HashSet<ServerInfo>();
            (request.Sources as List<ServerInfo>).AddRange(allMachines);
            
            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(requestMessage =>
                    {
                        var requestedSources = MockDataFactory.UnpackRequest<TieredRequest>(requestMessage).Result;
                        lock (allMachines)
                        {
                            var leaderName = MockDataFactory.ExtractServerInfo(requestMessage.RequestUri);
                            requestedSources.Sources.Add(leaderName);

                            foreach (var name in requestedSources.Sources)
                            {
                                Assert.IsTrue(seenMachines.Add(name));
                            }
                        }
                                                                                   
                        // causes the request to fail (we don't care about the request here, we are just inspecting the request)
                        throw new WebException();
                    });


            var response = await this.client.CounterQuery("/something", request, null);
            Assert.IsNotNull(response);
            Assert.AreEqual(allMachines.Count, seenMachines.Count);
            Assert.IsTrue(allMachines.All(seenMachines.Contains));
        }

        [Test]
        public async void ServerTimesOut()
        {
            // emulate canceled as an OperationCanceledException
            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(_ => { throw new WebException("", WebExceptionStatus.Timeout); });

            const int maxFanout = 2;
            const int totalMachinesToQuery = 10;

            var response = await this.client.CounterQuery("/soemthing", CreateRequest(totalMachinesToQuery, maxFanout), null);
            Assert.IsNotNull(response);

            // no samples since it failed
            Assert.IsEmpty(response.Samples);
            Assert.AreEqual(totalMachinesToQuery, response.RequestDetails.Count);

            // the machines that were queries should be marked as timed out, the rest should be 'federation error' since we don't know
            Assert.AreEqual(maxFanout, response.RequestDetails.Count(item => item.Status == RequestStatus.TimedOut));
            Assert.AreEqual(totalMachinesToQuery - maxFanout, response.RequestDetails.Count(item => item.Status == RequestStatus.FederationError));
        }

        [Test]
        public async void ServerReturnsErrorCode()
        {
            // emulate a failure response
            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(_ => new HttpResponseMessage(HttpStatusCode.PaymentRequired) {Content = new StringContent("Pay up")});

            const int maxFanout = 2;
            const int totalMachinesToQuery = 10;

            var response =
                await this.client.CounterQuery("/something", CreateRequest(totalMachinesToQuery, maxFanout), null);
            Assert.IsNotNull(response);

            // no samples since it failed
            Assert.IsEmpty(response.Samples);
            Assert.AreEqual(totalMachinesToQuery, response.RequestDetails.Count);

            // we did not get diagnostic information so unknown servers should give us federation error
            Assert.AreEqual(maxFanout, response.RequestDetails.Count(item => item.Status == RequestStatus.ServerFailureResponse));
            Assert.AreEqual(totalMachinesToQuery - maxFanout, response.RequestDetails.Count(item => item.Status == RequestStatus.FederationError));
        }

        [Test]
        public async void ServerReturnsErrorCodeWithDiagnostics()
        {
            const HttpStatusCode expectedStatusCode = HttpStatusCode.PaymentRequired;
            const RequestStatus expectedOtherResponse = RequestStatus.RequestException;

            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(requestMessage => GenerateFailureResponse(
                                                                                            requestMessage,
                                                                                            expectedStatusCode,
                                                                                            expectedOtherResponse).Result);

            const int maxFanout = 1;
            const int totalMachinesToQuery = 10;

            var response = await this.client.CounterQuery("/soemthing", CreateRequest(totalMachinesToQuery, maxFanout), null);
            Assert.IsNotNull(response);

            // no samples since it failed
            Assert.IsEmpty(response.Samples);
            Assert.AreEqual(totalMachinesToQuery, response.RequestDetails.Count);

            // we should get all the responses we care about
            Assert.AreEqual(maxFanout, response.RequestDetails.Count(item => item.Status == RequestStatus.ServerFailureResponse && item.HttpResponseCode == (int)expectedStatusCode));
            Assert.AreEqual(totalMachinesToQuery - maxFanout, response.RequestDetails.Count(item => item.Status == expectedOtherResponse));
        }

        [Test]
        public async void ServerConnectionFails()
        {
            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(_ => { throw new WebException("", WebExceptionStatus.ConnectionClosed); });

            const int maxFanout = 2;
            const int totalMachinesToQuery = 10;

            var response = await this.client.CounterQuery("/something", CreateRequest(totalMachinesToQuery, maxFanout), null);
            Assert.IsNotNull(response);

            // no samples since it failed
            Assert.IsEmpty(response.Samples);
            Assert.AreEqual(totalMachinesToQuery, response.RequestDetails.Count);

            // the machines that were queries should be marked as timed out, the rest should be 'federation error' since we don't know
            Assert.AreEqual(maxFanout, response.RequestDetails.Count(item => item.Status == RequestStatus.RequestException));
            Assert.AreEqual(totalMachinesToQuery - maxFanout, response.RequestDetails.Count(item => item.Status == RequestStatus.FederationError));
        }

        [Test]
        public async void InvalidDataReturned()
        {
            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(_ => new HttpResponseMessage(HttpStatusCode.OK)
                                                                                        {
                                                                                            Content = new ByteArrayContent(new byte[] {0xa})
                                                                                        });

            const int maxFanout = 2;
            const int totalMachinesToQuery = 10;

            var response = await this.client.CounterQuery("/something", CreateRequest(totalMachinesToQuery, maxFanout), null);
            Assert.IsNotNull(response);

            // no samples since it failed
            Assert.IsEmpty(response.Samples);
            Assert.AreEqual(totalMachinesToQuery, response.RequestDetails.Count);

            // the machines that were queries should be marked as bad data, the rest should be 'federation error' since we don't know
            Assert.AreEqual(maxFanout, response.RequestDetails.Count(item => item.Status == RequestStatus.RequestException));
            Assert.AreEqual(totalMachinesToQuery - maxFanout, response.RequestDetails.Count(item => item.Status == RequestStatus.FederationError));
        }

        [Test]
        public void UnexpectedExceptionIsNotCaught()
        {
            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(_ => { throw new TypeUnloadedException("No...Mr. Superman no home"); });

            const int maxFanout = 2;
            const int totalMachinesToQuery = 10;

            Assert.Throws<TypeUnloadedException>(async () => await this.client.CounterQuery("/something", CreateRequest(totalMachinesToQuery, maxFanout), null));
        }

        [Test]
        public async void DataIsMergedProperly()
        {
            var nfcChampionship = new DateTime(2015, 01, 18);
            const int numSampleBuckets = 10;

            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(requestMessage => MockDataFactory.CreateGoodHitCountResponse(
                                                                                            requestMessage,
                                                                                            nfcChampionship,
                                                                                            numSampleBuckets).Result);

            const int maxFanout = 2;
            const int totalMachinesToQuery = 10;

            var response = await this.client.CounterQuery("/something", CreateRequest(totalMachinesToQuery, maxFanout), null);
            Assert.IsNotNull(response);

            Assert.AreEqual(totalMachinesToQuery, response.RequestDetails.Count);
            Assert.AreEqual(totalMachinesToQuery, response.RequestDetails.Count(s => s.Status == RequestStatus.Success));

            Assert.AreEqual(numSampleBuckets, response.Samples.Count);
            Assert.IsTrue(response.Samples.All(sample => sample.HitCount == totalMachinesToQuery));
        }

        [Test]
        public async void DistributedQueryClientStripsPercentileOutOfQueryParameter()
        {
            bool didTestPass = false;

            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(requestMessage =>
                                                                                   {
                                                                                       Assert.AreEqual(string.Empty, requestMessage.RequestUri.Query);
                                                                                       didTestPass = true;
                                                                                       return MockDataFactory.CreateFailedTieredResponse("me", HttpStatusCode.BadRequest);
                                                                                   });

            // ensure the distributed client removed the 'percentile = xxx' parameter in the request
            var response =
                await
                this.client.CounterQuery("/something", CreateRequest(10, 5),
                                         new Dictionary<string, string> {{"Percentile", "45.1243"}});
            Assert.IsNotNull(response);

            Assert.IsTrue(didTestPass);
        }

        [Test]
        public async void MixedStatusAndFailureResponses()
        {
            var nfcChampionship = new DateTime(2015, 01, 18);
            const int numSampleBuckets = 10;

            int callbackNumber = 0;

            // N - 1 successes. 1 failure (timeout)
            DistributedQueryClient.RequesterFactory = new MockHttpRequesterFactory(requestMessage =>
                        {
                            var isFirst = Interlocked.Increment(ref callbackNumber) == 1;
                            if (isFirst)
                            {
                                throw new OperationCanceledException("timed out");
                            }

                            return MockDataFactory.CreateGoodHitCountResponse(
                                requestMessage,
                                nfcChampionship,
                                numSampleBuckets).Result;

                        });
                
                
            const int maxFanout = 3;
            const int totalMachinesToQuery = 9;
            const int machinesToSucceed = totalMachinesToQuery - maxFanout;
            const int machinesToTimeout = 1;
            const int machinesWithFederationError = totalMachinesToQuery - machinesToSucceed - machinesToTimeout;

            var response = await this.client.CounterQuery("/something", CreateRequest(totalMachinesToQuery, maxFanout), null);
            Assert.IsNotNull(response);

            Assert.AreEqual(totalMachinesToQuery, response.RequestDetails.Count);
            Assert.AreEqual(machinesToSucceed, response.RequestDetails.Count(s => s.Status == RequestStatus.Success));
            Assert.AreEqual(machinesToTimeout, response.RequestDetails.Count(s => s.Status == RequestStatus.TimedOut));
            Assert.AreEqual(machinesWithFederationError, response.RequestDetails.Count(s => s.Status == RequestStatus.FederationError));

            Assert.AreEqual(numSampleBuckets, response.Samples.Count);
            Assert.IsTrue(response.Samples.All(sample => sample.HitCount == machinesToSucceed));
        }

        private static async Task<HttpResponseMessage> GenerateFailureResponse(HttpRequestMessage request,
                                                                               HttpStatusCode responseCode,
                                                                               RequestStatus statusForDownstreamSources)
        {
            var requestMessage = await MockDataFactory.UnpackRequest<TieredRequest>(request);
            var responseContent = new CounterQueryResponse();
            responseContent.RequestDetails = requestMessage.Sources.Select(source =>
                                                                           new RequestDetails
                                                                           {
                                                                               Server = source,
                                                                               Status = statusForDownstreamSources
                                                                           }).ToList();

            // add one for the leader
            responseContent.RequestDetails.Add(new RequestDetails
            {
                Server = MockDataFactory.ExtractServerInfo(request.RequestUri),
                Status = RequestStatus.ServerFailureResponse,
                HttpResponseCode = (short)responseCode
            });
            return MockDataFactory.CreateResponse(responseContent, responseCode);
        }

        private static TieredRequest CreateRequest(int sourceCount, int maxFanout)
        {
            var request = new TieredRequest
                          {
                              IncludeRequestDiagnostics = true,
                              FanoutTimeoutInMilliseconds = DefaultTimeOut.Milliseconds,
                              MaxFanout = maxFanout
                          };
            for (int i = 0; i < sourceCount; i++)
            {
                request.Sources.Add(new ServerInfo {Hostname = "Source_" + i, Port = 1});
            }

            return request;
        }
    }
}
