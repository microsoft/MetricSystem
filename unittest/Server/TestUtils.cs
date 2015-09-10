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
    using System.Net;
    using System.Net.Http;

    using MetricSystem.Data;

    using NUnit.Framework;

    public static class TestUtils
    {
        public static Uri GetUri(Server server, string commandName, string counterName)
        {
            return GetUri(server, commandName, counterName, null, new Dictionary<string, string>());
        }

        public static Uri GetUri(Server server, string commandName, string counterName, string subCommandName)
        {
            return GetUri(server, commandName, counterName, subCommandName, new Dictionary<string, string>());
        }

        public static Uri GetUri(Server server, string commandName, string counterName,
                                  IEnumerable<KeyValuePair<string, string>> parameters)
        {
            return GetUri(server, commandName, counterName, null, parameters);
        }

        public static Uri GetUri(Server server, string commandName, string counterName, string subCommandName,
                                  IEnumerable<KeyValuePair<string, string>> parameters)
        {
            var queryString = "?";
            foreach (var kvp in parameters)
            {
                if (queryString.Length > 1)
                {
                    queryString += '&';
                }
                queryString += kvp.Key + '=' + kvp.Value;
            }

            var path = commandName;
            path += counterName;
            if (!string.IsNullOrEmpty(subCommandName))
            {
                path += '/' + subCommandName;
            }

            return new UriBuilder("http", "localhost", server.Port, path,
                                  queryString.Length > 1 ? queryString : string.Empty).Uri;
        }
    }

    public abstract class RequestHandlerTestBase
    {
        protected const string AnyCounter = "/AnyCounter";
        protected DataManager dataManager;
        protected HttpClient httpClient;
        protected Server server;

        [SetUp]
        public void BaseSetUp()
        {
            this.dataManager = new DataManager();
            this.server = new Server("localhost", 0, this.dataManager, this.GetRequestHandlers(), 0);
            this.server.Start();
            this.httpClient = new HttpClient(
                new WebRequestHandler
                {
                    AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip
                });
            this.httpClient.DefaultRequestHeaders.Add("Accept", Protocol.BondCompactBinaryMimeType);

            this.Setup();
        }

        [TearDown]
        public void BaseTearDown()
        {
            this.server.Dispose();
            this.httpClient.Dispose();
            this.Cleanup();
        }

        public virtual void Setup() { }
        public virtual void Cleanup() { }
        public virtual IList<RequestHandler> GetRequestHandlers()
        {
            return null;
        }
    }
}
