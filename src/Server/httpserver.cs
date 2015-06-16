// ---------------------------------------------------------------------
// <copyright file="HttpServer.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
            get { return this.listener.IsListening; }
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
                if (this.listener == null || !this.Running)
                {
                    return;
                }

                var context = this.listener.EndGetContext(result);
                OnRequestReceived(context);
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
