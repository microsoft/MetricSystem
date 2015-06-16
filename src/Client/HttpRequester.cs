
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
