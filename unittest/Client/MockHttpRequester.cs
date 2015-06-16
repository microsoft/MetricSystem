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

namespace MetricSystem.Client.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Threading.Tasks;

    using MetricSystem.Utilities;

    internal class MockHttpRequesterFactory : IHttpRequesterFactory
    {
        internal readonly Func<HttpRequestMessage, HttpResponseMessage> callbackFunc;

        internal MockHttpRequesterFactory(Func<HttpRequestMessage, HttpResponseMessage> callbackFunc)
        {
            this.callbackFunc = callbackFunc;
        }

        public IHttpRequester GetRequester()
        {
            return new MockHttpRequester(this.callbackFunc);
        }

        internal class MockHttpRequester : IHttpRequester
        {
            internal readonly Func<HttpRequestMessage, HttpResponseMessage> UserFunc;

            public MockHttpRequester(Func<HttpRequestMessage, HttpResponseMessage> callbackFunc)
            {
                this.UserFunc = callbackFunc;
            }

            public async Task<HttpResponseMessage> StartRequest(HttpRequestMessage request)
            {
                return await Task.Factory.StartNew(() => this.UserFunc(request));
            }

            public TimeSpan Timeout
            {
                get { return TimeSpan.FromSeconds(100); }
                set { }
            }

            public void Dispose()
            {
                // toddse: hahaha
                // clocke: I loled
            }
        }
    }

    internal static class MockDataFactory
    {
        internal static HttpResponseMessage CreateResponse<T>(T response, HttpStatusCode statusCode = HttpStatusCode.OK)
        {
            var result = new HttpResponseMessage(statusCode);
            using (var tempStream = new MemoryStream())
            {
                using (var writerStream = new WriterStream(tempStream))
                {
                    writerStream.CreateCompactBinaryWriter().Write(response);
                    result.Content = new ByteArrayContent(tempStream.GetBuffer());
                }
            }
            return result;
        }

        internal static HttpResponseMessage CreateFailedTieredResponse(string hostname, HttpStatusCode status)
        {
            var response = new CounterQueryResponse();
            response.RequestDetails.Add(new RequestDetails
                                        {
                                            Server = new ServerInfo
                                                     {
                                                         Hostname = hostname,
                                                         Port = 42,
                                                     },
                                            HttpResponseCode = (short)status,
                                            Status = RequestStatus.ServerFailureResponse,
                                            IsAggregator = false,
                                            StatusDescription = "Not found"
                                        });

            return CreateResponse(response, HttpStatusCode.NotFound);
        }

        internal static HttpResponseMessage CreateGoodBatchQueryResponse()
        {
            var response = new BatchQueryResponse();
            response.Responses.Add(new CounterQueryResponse {HttpResponseCode = 200});

            return CreateResponse(response);
        }

        internal static HttpResponseMessage CreateWrongResponseType()
        {
            return CreateResponse(new CounterInfoResponse());
        }

        internal static HttpResponseMessage CreateGoodCounterResponse(DateTime startTime, int numBuckets)
        {
            var CounterQueryResponse = new CounterQueryResponse {Samples = new List<DataSample>()};
            AddHitCountSamples(CounterQueryResponse.Samples, startTime, numBuckets, 1);

            return CreateResponse(CounterQueryResponse);
        }

        internal static void AddHitCountSamples(IList<DataSample> sampleList, DateTime startTime, int numBuckets,
                                                uint hitPerBucket)
        {
            for (int i = 0; i < numBuckets; i++)
            {
                sampleList.Add(
                               new DataSample
                               {
                                   Name = i.ToString(),
                                   HitCount = hitPerBucket,
                                   SampleType = DataSampleType.HitCount,
                                   StartTime = startTime.AddMinutes(i).ToMillisecondTimestamp(),
                                   EndTime = startTime.AddMinutes(i + 1).ToMillisecondTimestamp()
                               });
            }
        }

        internal static HttpResponseMessage CreateErrorResponse()
        {
            var response = new BatchQueryResponse();
            response.Responses.Add(new CounterQueryResponse {HttpResponseCode = 404});

            return CreateResponse(response);
        }

        internal static ServerInfo ExtractServerInfo(Uri requestUri)
        {
            return new ServerInfo {Hostname = requestUri.Host, Port = (ushort)requestUri.Port};
        }

        internal static async Task<HttpResponseMessage> CreateGoodHitCountResponse(HttpRequestMessage request,
                                                                                   DateTime startTime,
                                                                                   int numSampleBuckets)
        {
            var requestMessage = await UnpackRequest<TieredRequest>(request);
            var responseContent = new CounterQueryResponse
                                  {
                                      Samples = new List<DataSample>(),
                                      RequestDetails = requestMessage.Sources.Select(source =>
                                                                                     new RequestDetails
                                                                                     {
                                                                                         Server = source,
                                                                                         Status = RequestStatus.Success
                                                                                     }).ToList()
                                  };

            // add a response for the leader
            responseContent.RequestDetails.Add(new RequestDetails
                                               {
                                                   Server = ExtractServerInfo(request.RequestUri),
                                                   Status = RequestStatus.Success,
                                                   HttpResponseCode = 200
                                               });

            AddHitCountSamples(responseContent.Samples, startTime, numSampleBuckets,
                               (uint)requestMessage.Sources.Count + 1);

            return CreateResponse(responseContent);
        }

        internal static async Task<TRequest> UnpackRequest<TRequest>(HttpRequestMessage request)
        {
            using (var requestData = new MemoryStream())
            {
                await request.Content.CopyToAsync(requestData);
                requestData.Position = 0;

                using (var reader = ReaderStream.FromMemoryStreamBuffer(requestData, null))
                {
                    var cbReader = reader.CreateCompactBinaryReader();
                    return cbReader.Read<TRequest>();
                }
            }
        }

        internal static async Task<TData> ReadResponseData<TData>(HttpResponseMessage response)
            where TData : class
        {
            using (var readerStream = new ReaderStream(await response.Content.ReadAsStreamAsync()))
            {
                return readerStream.CreateCompactBinaryReader().Read<TData>();
            }
        }
    }
}
