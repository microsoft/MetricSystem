// ---------------------------------------------------------------------
// <copyright file="Request.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Server
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Threading.Tasks;
    using System.Web;

    using Bond;

    using Microsoft.IO;
    using MetricSystem.Utilities;

    public sealed class Request
    {
        private readonly RecyclableMemoryStreamManager memoryStreamManager;

        internal Request(HttpListenerContext context, RecyclableMemoryStreamManager memoryStreamManager,
                         Server server, RequestHandler handler)
        {
            this.Context = context;
            this.Requester = context.Request.RemoteEndPoint;

            this.memoryStreamManager = memoryStreamManager;
            this.Server = server;
            this.Handler = handler;

            this.Path = HttpUtility.UrlDecode(context.Request.Url.AbsolutePath.Substring(this.Handler.Prefix.Length));

            this.QueryParameters = new Dictionary<string, string>();
            var parsedParams = HttpUtility.ParseQueryString(context.Request.Url.Query);
            foreach (var key in parsedParams.AllKeys)
            {
                if (key != null) // a null key is provided for strings like '?hello' with no assignment to a name.
                {
                    this.QueryParameters.Add(key, parsedParams.Get(key));
                }
            }

            this.Headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var header in context.Request.Headers.AllKeys)
            {
                var value = context.Request.Headers.Get(header);
                this.Headers.Add(header, value);
            }
        }

        internal RequestHandler Handler { get; private set; }

        internal Server Server { get; private set; }

        internal IPEndPoint Requester { get; private set; }

        internal HttpListenerContext Context { get; private set; }

        /// <summary>
        /// The path portion of the URI, excluding the leading part from the associated request Handler.
        /// </summary>
        public string Path { get; private set; }

        /// <summary>
        /// Query parameters sent on the URL. Guaranteed to never be null.
        /// </summary>
        public IDictionary<string, string> QueryParameters { get; private set; }

        /// <summary>
        /// Headers provided as part of the request.
        /// </summary>
        public IDictionary<string, string> Headers { get; private set; }

        /// <summary>
        /// True if the request has accompanying data (e.g. a POST body).
        /// </summary>
        public bool HasInputBody
        {
            get { return this.Context.Request.HasEntityBody; }
        }

        public MemoryStream GetStream()
        {
            return this.GetStream(0);
        }

        public MemoryStream GetStream(long length)
        {
            return this.memoryStreamManager.GetStream(this.Handler.Prefix, (int)length);
        }

        public Response CreateErrorResponse(HttpStatusCode statusCode, string errorMessage)
        {
            Events.Write.SendingErrorResponse(this, (int)statusCode, errorMessage);
            return new Response(this, statusCode, errorMessage ?? "Unknown error.");
        }

        /// <summary>
        /// Reads a Bond object from the input data sent with the request.
        /// </summary>
        /// <typeparam name="TValue">The type to use for deserialization.</typeparam>
        /// <returns>The object deserialized, or a default value if nothing can be read.</returns>
        public async Task<TValue> ReadInputBody<TValue>() where TValue : class, new()
        {
            if (!this.HasInputBody)
            {
                return default(TValue);
            }

            using (var ms = this.GetStream(this.Context.Request.ContentLength64))
            {
                await this.Context.Request.InputStream.CopyToAsync(ms);
                ms.Position = 0;
                using (var reader = ReaderStream.FromMemoryStreamBuffer(ms, this.memoryStreamManager))
                {
                    try
                    {
                        string contentType;
                        if (!this.Headers.TryGetValue("Content-Type", out contentType))
                        {
                            contentType = Protocol.BondCompactBinaryMimeType;
                        }

                        switch (contentType)
                        {
                        case Protocol.ApplicationJsonMimeType:
                            return Deserialize<TValue>.From(reader.CreateSimpleJsonReader());

                        case Protocol.BondCompactBinaryMimeType:
                            return reader.CreateCompactBinaryReader().Read<TValue>();

                        default:
                            Events.Write.InvalidContentTypeProvided(this, contentType);
                            return default(TValue);
                        }
                    }
                    catch (Exception ex)
                    {
                        if (ex is InvalidDataException || ex is EndOfStreamException || ex is IOException)
                        {
                            return default(TValue);
                        }

                        throw;
                    }
                }
            }
        }
    }
}
