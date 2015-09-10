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

namespace MetricSystem.Utilities.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.Sockets;
    using System.Text;
    using System.Threading;

    public sealed class TestHttpServer
    {
        public delegate HttpStatusCode RequestHook(HttpListenerContext context);

        public delegate void SendingResponseHook(HttpListenerResponse response);

        /// <summary>
        /// Set this to a string response body to use.
        /// </summary>
        public string ResponseMessage { get; set; }

        /// <summary>
        /// Set any headers to return for requests.
        /// </summary>
        public Dictionary<string, string> ResponseHeaders { get; set; }

        /// <summary>
        /// Set this to do something when a request is received.
        /// </summary>
        public RequestHook GotRequest;

        /// <summary>
        /// Set this to do something with the response before it is sent back.
        /// </summary>
        public SendingResponseHook SendingResponse;

        private HttpListener listener;
        private int listening;

        public TestHttpServer()
        {
            this.ResponseHeaders = new Dictionary<string, string>();
            this.ResponseMessage = string.Empty;

            var socket = new TcpListener(IPAddress.Loopback, 0);
            socket.Start();
            this.Port = ((IPEndPoint)socket.LocalEndpoint).Port;
            socket.Stop();

            this.listener = new HttpListener();
            this.listener.Prefixes.Add(string.Format("http://localhost:{0}/", this.Port));
            this.listener.IgnoreWriteExceptions = true;
            this.listener.AuthenticationSchemeSelectorDelegate += AuthenticationSchemeRuntimeSelector;
            this.listener.Start();
            this.listening = 1;

            this.GetNextRequest();
        }

        public int Port { get; private set; }

        public void Shutdown()
        {
            Interlocked.Exchange(ref this.listening, 0);
            this.listener.Stop();
            this.listener = null;
        }

        private void GetNextRequest()
        {
            if (Interlocked.CompareExchange(ref this.listening, 1, 1) == 1)
            {
                this.listener.BeginGetContext(this.EndGetContext, null);
            }
        }

        private void EndGetContext(IAsyncResult result)
        {
            this.GetNextRequest();
            if (result != null && Interlocked.CompareExchange(ref this.listening, 1, 1) == 1)
            {
                try
                {
                    this.HandleRequest(this.listener.EndGetContext(result));
                }
                catch (HttpListenerException) { }
                catch (ObjectDisposedException) { }
            }
        }

        private void HandleRequest(HttpListenerContext context)
        {
            var responseCode = HttpStatusCode.OK;
            if (this.GotRequest != null)
            {
                responseCode = this.GotRequest(context);
            }

            if (responseCode == 0)
            {
                context.Response.Abort();
                return;
            }
            context.Response.StatusCode = (int)responseCode;

            foreach (var kvp in ResponseHeaders)
            {
                context.Response.AddHeader(kvp.Key, kvp.Value);
            }

            if (this.SendingResponse != null)
            {
                this.SendingResponse(context.Response);
            }

            var bytes = Encoding.UTF8.GetBytes(ResponseMessage);
            context.Response.OutputStream.Write(bytes, 0, bytes.Length);
            context.Response.Close();
        }

        private static AuthenticationSchemes AuthenticationSchemeRuntimeSelector(HttpListenerRequest request)
        {
            return AuthenticationSchemes.Anonymous;
        }
    }
}
