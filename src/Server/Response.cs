// ---------------------------------------------------------------------
// <copyright file="Response.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Server
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.IO.Compression;
    using System.Linq;
    using System.Net;
    using System.Net.Mime;
    using System.Text;
    using System.Threading.Tasks;

    using global::Bond.Protocols;

    using MetricSystem.Utilities;

    using Protocol = MetricSystem.Protocol;

    public sealed class Response
    {
        private readonly Request request;

        private MemoryStream responseBody;

        /// <summary>
        /// Create a response with a stream containing the pre-populated contents.
        /// </summary>
        /// <param name="request">The request being responded to.</param>
        /// <param name="statusCode">The status code to send back.</param>
        /// <param name="body">The data to send back.</param>
        public Response(Request request, HttpStatusCode statusCode, MemoryStream body)
        {
            this.request = request;
            this.HttpResponse.StatusCode = (int)statusCode;
            this.responseBody = body ?? this.request.GetStream();
            this.responseBody.Position = 0;
        }

        /// <summary>
        /// Create a response with a simple text body.
        /// </summary>
        /// <param name="request">The request being responded to.</param>
        /// <param name="statusCode">The status code to send back.</param>
        /// <param name="message">The message to send back.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public Response(Request request, HttpStatusCode statusCode, string message)
            : this(request, statusCode, request.GetStream())
        {
            var encoded = Encoding.UTF8.GetBytes(message);
            this.responseBody.Write(encoded, 0, encoded.Length);
            this.ContentType = MediaTypeNames.Text.Plain;
        }

        public static Response Create<TData>(Request request, HttpStatusCode statusCode, TData data)
        {
            var response = new Response(request, statusCode);
            response.Write(data);

            return response;
        }

        /// <summary>
        /// Create a response with a Bond object body.
        /// </summary>
        /// <param name="request">The request being responded to.</param>
        /// <param name="statusCode">The status code to send back.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design",
            "CA1062:Validate arguments of public methods", MessageId = "0"),
         System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design",
             "CA1062:Validate arguments of public methods", MessageId = "2")]
        private Response(Request request, HttpStatusCode statusCode)
            : this(request, statusCode, request.GetStream())
        {
        }

        private void Write<TData>(TData data)
        {
            var acceptTypes = this.HttpRequest.AcceptTypes;
            if (acceptTypes != null && acceptTypes.Contains(Protocol.BondCompactBinaryMimeType, StringComparer.Ordinal))
            {
                this.ContentType = Protocol.BondCompactBinaryMimeType;
                using (var writerStream = new WriterStream(this.responseBody))
                {
                    var writer = writerStream.CreateCompactBinaryWriter();
                    writer.Write(data);
                }
            }
            else
            {
                this.ContentType = Protocol.ApplicationJsonMimeType;
                var writer = new SimpleJsonWriter(this.responseBody);
                writer.Write(data);
            }

        }

        private HttpListenerRequest HttpRequest
        {
            get { return this.request.Context.Request; }
        }

        private HttpListenerResponse HttpResponse
        {
            get { return this.request.Context.Response; }
        }

        /// <summary>
        /// The content type of the response body. This is generally set for you.
        /// </summary>
        public string ContentType
        {
            get { return this.HttpResponse.ContentType; }
            set { this.HttpResponse.ContentType = value; }
        }

        public HttpStatusCode ResponseCode
        {
            get { return (HttpStatusCode)this.HttpResponse.StatusCode; }
            set { this.HttpResponse.StatusCode = (int)value; }
        }

        private const string AcceptEncoding = "Accept-Encoding";
        private const string ContentEncoding = "Content-Encoding";
        internal async Task Send()
        {
            var compressionType = ParseAcceptEncoding(this.HttpRequest.Headers.Get(AcceptEncoding));

            if (this.responseBody.Length < this.request.Server.MinimumResponseSizeToCompress)
            {
                compressionType = ResponseCompressionType.None;
            }

            Stream compressorStream = null;
            MemoryStream originalStream = this.responseBody;
            switch (compressionType)
            {
            case ResponseCompressionType.GZip:
                this.responseBody = this.request.GetStream();
                this.HttpResponse.AddHeader(ContentEncoding, "gzip");
                compressorStream = new GZipStream(this.responseBody, CompressionMode.Compress, true);
                break;
            case ResponseCompressionType.Deflate:
                this.responseBody = this.request.GetStream();
                this.HttpResponse.AddHeader(ContentEncoding, "deflate");
                compressorStream = new DeflateStream(this.responseBody, CompressionMode.Compress, true);
                break;
            }

            if (compressorStream != null)
            {
                this.HttpResponse.AddHeader("UncompressedLength",
                                            originalStream.Length.ToString(CultureInfo.InvariantCulture));

                var uncompressedBytes = originalStream.GetBuffer();
                using (compressorStream)
                {
                    Events.Write.BeginCompressingResponse(uncompressedBytes.Length);
                    await compressorStream.WriteAsync(uncompressedBytes, 0, uncompressedBytes.Length);
                    Events.Write.CompleteCompressingResponse(this.responseBody.Length);
                }
                originalStream.Dispose();
            }

            try
            {
                this.responseBody.Position = 0;
                await this.responseBody.CopyToAsync(this.HttpResponse.OutputStream);
            }
            finally
            {
                this.responseBody.Dispose();
            }
        }

        #region Encoding parsing
        private const string GZipEncodingName = "gzip";
        private const string DeflateEncodingName = "deflate";        

        private enum ResponseCompressionType
        {
            None,
            GZip,
            Deflate,
        }

        private enum ParsingState
        {
            Start,
            ReadName,
            AfterName,
            WaitNextToken,
            StartQValue,
            AfterQValueName,
            StartQValueValue,
            AfterQValueIntegerPart,
            ReadQValue,
            AfterQValue,
        }

        // This parser is difficult to read, but does not generate too many unwanted temporary strings.
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502")]
        private static ResponseCompressionType ParseAcceptEncoding(string acceptEncoding)
        {
            if (string.IsNullOrEmpty(acceptEncoding))
            {
                return ResponseCompressionType.None;
            }

            // The Accept-Encoding header is a comma separated list of encodings.
            // Each encoding is composed of a name followed by an optional section in the form "; q=0.XXX".
            // The following code will get all the supported encoding, filter out the ones that do not
            // match with a supported compression type and return the compression type with higher qvalue.
            var result = ResponseCompressionType.None;
            float maxValue = 0;

            var currentToken = new StringBuilder(acceptEncoding.Length);
            var state = ParsingState.Start;
            var type = ResponseCompressionType.None;
            float currentValue = 0;
            float currentMultiplier = 1;
            for (int index = 0; index < acceptEncoding.Length; index += 1)
            {
                char currentChar = acceptEncoding[index];
                switch (state)
                {
                    case ParsingState.Start:
                        if (char.IsWhiteSpace(currentChar))
                        {
                            continue;
                        }
                        if (',' == currentChar)
                        {
                            // This is an empty entry in the list, it's an error, but we will
                            // just skip it and wait for the next entry.
                            continue;
                        }
                        if (';' == currentChar)
                        {
                            // This is another error, skip the content of this entry and
                            // wait for ',' to start another token
                            state = ParsingState.WaitNextToken;
                            continue;
                        }
                        type = ResponseCompressionType.None;
                        state = ParsingState.ReadName;
                        currentToken.Clear();
                        currentToken.Append(currentChar);
                        break;

                    case ParsingState.WaitNextToken:
                        if (',' == currentChar)
                        {
                            state = ParsingState.Start;
                        }
                        break;

                    case ParsingState.ReadName:
                        if (',' == currentChar)
                        {
                            // We have found a name without a qvalue, so we assume 1 as value.
                            type = MapEncodingToComporessionType(currentToken.ToString());
                            if (type != ResponseCompressionType.None)
                            {
                                // There is no need to look at the other elements because no one
                                // can have a qvalue grater than 1, so we just return the current type.
                                return type;
                            }
                            state = ParsingState.Start;
                        }
                        else if (';' == currentChar)
                        {
                            type = MapEncodingToComporessionType(currentToken.ToString());
                            if (type == ResponseCompressionType.None)
                            {
                                state = ParsingState.WaitNextToken;
                            }
                            else
                            {
                                state = ParsingState.StartQValue;
                            }
                        }
                        else if (char.IsWhiteSpace(currentChar))
                        {
                            // We don't allow spaces inside the name, so we can try to match the name
                            // as is now.
                            type = MapEncodingToComporessionType(currentToken.ToString());
                            if (type == ResponseCompressionType.None)
                            {
                                // This encoding is not supported, so there is no reason to
                                // parse anything else inside this entry.
                                state = ParsingState.WaitNextToken;
                                continue;
                            }
                            else
                            {
                                state = ParsingState.AfterName;
                            }
                        }
                        else
                        {
                            currentToken.Append(currentChar);
                        }
                        break;

                    case ParsingState.AfterName:
                        if (';' == currentChar)
                        {
                            state = ParsingState.StartQValue;
                        }
                        else if (',' == currentChar)
                        {
                            return type;
                        }
                        else if (!char.IsWhiteSpace(currentChar))
                        {
                            // Any other char is an error in this state, so we skip the current entry.
                            state = ParsingState.WaitNextToken;
                        }
                        break;

                    case ParsingState.StartQValue:
                        if ('q' == currentChar || 'Q' == currentChar)
                        {
                            state = ParsingState.AfterQValueName;
                        }
                        else if (!char.IsWhiteSpace(currentChar))
                        {
                            state = ParsingState.WaitNextToken;
                        }
                        break;

                    case ParsingState.AfterQValueName:
                        if ('=' == currentChar)
                        {
                            state = ParsingState.StartQValueValue;
                        }
                        else if (!char.IsWhiteSpace(currentChar))
                        {
                            state = ParsingState.WaitNextToken;
                        }
                        break;

                    case ParsingState.StartQValueValue:
                        if ('0' == currentChar)
                        {
                            currentValue = 0;
                            state = ParsingState.AfterQValueIntegerPart;
                        }
                        else if ('1' == currentChar)
                        {
                            currentValue = 1;
                            state = ParsingState.AfterQValueIntegerPart;
                        }
                        else if (!char.IsWhiteSpace(currentChar))
                        {
                            state = ParsingState.WaitNextToken;
                        }
                        break;

                    case ParsingState.AfterQValueIntegerPart:
                        if ('.' == currentChar)
                        {
                            currentMultiplier = 0.1F;
                            state = ParsingState.ReadQValue;
                        }
                        else if (',' == currentChar)
                        {
                            // After the if we habe just read the integer part we have only two possibility
                            // for the value: 0 or 1. If it's zero we have to drop the value, if it's 1 this
                            // is the final result.
                            if (currentValue == 1)
                            {
                                return type;
                            }
                            state = ParsingState.Start;
                        }
                        else if (char.IsWhiteSpace(currentChar))
                        {
                            state = ParsingState.AfterQValue;
                        }
                        else
                        {
                            state = ParsingState.WaitNextToken;
                        }
                        break;

                    case ParsingState.ReadQValue:
                        {
                            int digitValue = (int)currentChar - (int)'0';
                            if (digitValue >= 0 && digitValue <= 9)
                            {
                                currentValue += (float)(digitValue * currentMultiplier);
                                currentMultiplier = currentMultiplier / 10;
                                if (currentMultiplier < 0.001F || currentValue > 1)
                                {
                                    // According with http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.9 only
                                    // three decimal digits are allow and the values must be between 0 and 1.
                                    state = ParsingState.WaitNextToken;
                                }
                            }
                            else if (char.IsWhiteSpace(currentChar))
                            {
                                state = ParsingState.AfterQValue;
                            }
                            else if (',' == currentChar)
                            {
                                if (currentValue > maxValue)
                                {
                                    result = type;
                                    maxValue = currentValue;
                                }
                                state = ParsingState.Start;
                            }
                            else
                            {
                                state = ParsingState.WaitNextToken;
                            }
                        }
                        break;

                    case ParsingState.AfterQValue:
                        if (',' == currentChar)
                        {
                            if (currentValue > maxValue)
                            {
                                result = type;
                                maxValue = currentValue;
                            }
                            state = ParsingState.Start;
                        }
                        else if (!char.IsWhiteSpace(currentChar))
                        {
                            state = ParsingState.WaitNextToken;
                        }
                        break;
                }
            }

            // once the loop is done we still have to check the state to handle the success cases.
            switch (state)
            {
                case ParsingState.AfterName:
                    // we have parsed the name, found it correct and there is no qvalue for it, so
                    // its value is 1 and we have not found other entries with value 1 before, otherwise
                    // we will have returned before the end of the loop. So this is the best match.
                    return type;

                case ParsingState.ReadName:
                    type = MapEncodingToComporessionType(currentToken.ToString());
                    if (type != ResponseCompressionType.None)
                    {
                        return type;
                    }
                    break;

                case ParsingState.ReadQValue:
                case ParsingState.AfterQValue:
                case ParsingState.AfterQValueIntegerPart:
                    if (currentValue > maxValue)
                    {
                        result = type;
                    }
                    break;
            }

            return result;
        }

        private static ResponseCompressionType MapEncodingToComporessionType(string name)
        {
            if (0 == string.Compare(name, GZipEncodingName, StringComparison.OrdinalIgnoreCase))
            {
                return ResponseCompressionType.GZip;
            }
            if (0 == string.Compare(name, DeflateEncodingName, StringComparison.OrdinalIgnoreCase))
            {
                return ResponseCompressionType.Deflate;
            }
            return ResponseCompressionType.None;
        }
        #endregion
    }
}
