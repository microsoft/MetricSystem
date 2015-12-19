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

namespace MetricSystem.Server
{
    using System;
    using System.Net;

    internal sealed class HttpServer : IDisposable
    {
        public delegate void RequestReceivedHandler(HttpServer server, HttpListenerContext requestContext);

        private bool disposed;
        private HttpListener listener;

        public HttpServer(string hostname, ushort port)
        {
            this.listener = new HttpListener();
            this.listener.Prefixes.Add(string.Format("http://{0}:{1}/", hostname, port));
            this.listener.IgnoreWriteExceptions = true;
        }

        public bool Running
        {
            get { return this.listener != null && this.listener.IsListening; }
        }

        public void Dispose()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException("HttpServer");
            }

            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void Start()
        {
            if (this.Running)
            {
                throw new InvalidOperationException("Server has already been started.");
            }

            this.listener.Start();
            this.ReceiveRequestAsync();
        }

        public void Stop()
        {
            if (this.Running)
            {
                this.listener.Stop();
            }
        }

        private void ReceiveRequestAsync()
        {
            try
            {
                this.listener.BeginGetContext(this.ProcessRequest, null);
            }
            catch (Exception ex)
            {
                Events.Write.HttpListenerException(ex);
                if (ex is HttpListenerException || ex is ObjectDisposedException || ex is InvalidOperationException
                    || (ex is ApplicationException && !this.Running))
                {
                    return;
                }

                throw;
            }
        }

        private void ProcessRequest(IAsyncResult result)
        {
            if (!this.Running)
            {
                return;
            }

            this.ReceiveRequestAsync();

            try
            {
                if (!this.Running)
                {
                    return;
                }

                var context = this.listener.EndGetContext(result);
                this.OnRequestReceived(context);
            }
            catch (Exception ex)
            {
                Events.Write.HttpListenerException(ex);
                if (ex is HttpListenerException || ex is ObjectDisposedException || ex is InvalidOperationException)
                {
                    return;
                }

                throw;
            }
        }

        private void OnRequestReceived(HttpListenerContext context)
        {
            if (this.RequestReceived != null)
            {
                this.RequestReceived(this, context);
            }
        }

        public event RequestReceivedHandler RequestReceived;

        ~HttpServer()
        {
            this.Dispose(false);
        }

        private void Dispose(bool disposing)
        {
            this.disposed = true;
            if (disposing && this.listener != null)
            {
                this.Stop();
                this.listener = null;
            }
        }
    }
}
