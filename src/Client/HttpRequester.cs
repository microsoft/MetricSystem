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

namespace MetricSystem.Client
{
    using System;
    using System.Net;
    using System.Net.Cache;
    using System.Net.Http;
    using System.Threading.Tasks;

    /// <summary>
    /// Interface interface which abstracts getting a requester to make actual http requests
    /// </summary>
    public interface IHttpRequesterFactory
    {
        IHttpRequester GetRequester();
    }

    /// <summary>
    /// Internal interface which abstracts the real HTTP request. Used for mocking in unit tests. 
    /// Default implementation uses the System.Net.Http library. 
    /// </summary>
    public interface IHttpRequester : IDisposable
    {
        TimeSpan Timeout { get; set; }

        Task<HttpResponseMessage> StartRequest(HttpRequestMessage request);
    }

    /// <summary>
    /// Default implementation of the 
    /// </summary>
    internal class HttpRequesterFactory : IHttpRequesterFactory
    {
        public IHttpRequester GetRequester()
        {
            return new HttpRequester();
        }
    }

    /// <summary>
    /// Default implementation of the IHttpRequester interface (which simply proxies to the System.Net.Http client)
    /// </summary>
    internal class HttpRequester : IHttpRequester
    {
        private readonly HttpClient client;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public HttpRequester()
        {
            var requestHandler = new WebRequestHandler
                                 {
                                     AllowPipelining = true,
                                     CachePolicy = new HttpRequestCachePolicy(HttpRequestCacheLevel.NoCacheNoStore),
                                     AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
                                 };

            this.client = new HttpClient(requestHandler, disposeHandler: true);
        }

        public TimeSpan Timeout
        {
            get { return client.Timeout; }
            set { client.Timeout = value; }
        }
        public Task<HttpResponseMessage> StartRequest(HttpRequestMessage request)
        {
            return client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        }

        public void Dispose()
        {
            // caller is responsible for not using this object after disposal. 
            // We proxy all requests to the client so there is nothing for us to do here
            client.Dispose();
        }
    }

}
