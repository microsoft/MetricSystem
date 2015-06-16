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
    using System.Collections.Generic;
    using System.Diagnostics.Tracing;
    using System.Net.Http;

    [EventSource(Guid = "{112f070b-eff4-4746-a1b4-9f4a68b8a32e}", Name = "MetricSystem-QueryClient")]
    sealed class Events : EventSource
    {
        public static Events Write = new Events();

        [Event(1, Level = EventLevel.Informational)]
        internal void BeginDistributedQuery(string command)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(1, command);
            }
        }

        [Event(2, Level = EventLevel.Informational)]
        internal void EndDistributedQuery(string command)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(2, command);
            }
        }

        [Event(3, Level = EventLevel.Informational)]
        public void SendingQuery(string serverName, string urlString)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(3, serverName, urlString);
            }
        }

        [NonEvent]
        public void SendingBlockQuery(ServerInfo server, string urlString, IEnumerable<ServerInfo> sources, double timeOutInMilliseconds)
        {
            if (this.IsEnabled())
            {
                this.SendingBlockQuery(server.Hostname, urlString, string.Join(",", sources), timeOutInMilliseconds);
            }
        }

        [Event(4, Level = EventLevel.Informational)]
        private void SendingBlockQuery(string hostName, string urlString, string sources, double timeoutInMilliseconds)
        {
            this.WriteEvent(4, hostName, urlString, sources, timeoutInMilliseconds);
        }

        [NonEvent]
        public void QueryFinished(ServerInfo server, string status, int httpStatusCode = 0, string diagnostics = null)
        {
            if (this.IsEnabled())
            {
                this.QueryFinished(server.ToString(), status, httpStatusCode, diagnostics);
            }
        }

        [Event(5, Level = EventLevel.Informational)]
        private void QueryFinished(string server, string status, int httpStatusCode = 0, string diagnostics = null)
        {
            this.WriteEvent(5, server, status, httpStatusCode, diagnostics);
        }

        [NonEvent]
        public void ReceivedResponseHeaders(ServerInfo server)
        {
            if (this.IsEnabled())
            {
                this.ReceivedResponseHeaders(server.ToString());
            }
        }

        [Event(6, Level = EventLevel.Verbose)]
        private void ReceivedResponseHeaders(string server)
        {
            this.WriteEvent(6, server);
        }

        [NonEvent]
        public void ReceivedResponseBody(ServerInfo server, long responseSize)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.ReceivedResponseBody(server.ToString(), responseSize);
            }
        }

        [Event(7, Level = EventLevel.Verbose)]
        private void ReceivedResponseBody(string server, long responseSize)
        {
            this.WriteEvent(7, server, responseSize);
        }


        [Event(9, Level = EventLevel.Error)]
        public void FatalExceptionWhenGatheringData(string message, string stackTrace)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(9, message, stackTrace);
            }
        }

        [Event(11, Level = EventLevel.Verbose)]
        internal void BeginBlockMerge()
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(11);
            }
        }

        [Event(12, Level = EventLevel.Verbose)]
        internal void CompleteBlockMerge()
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(12);
            }
        }

        [Event(13, Level = EventLevel.Verbose)]
        internal void CreateMergedResponse()
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(13);
            }
        }

        [Event(14, Level = EventLevel.Error)]
        internal void UnknownCounterResponse(string context)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(14, context);
            }
        }

        [NonEvent]
        public void QueryTimedOut(ServerInfo server)
        {
            this.QueryFinished(server.ToString(), RequestStatus.TimedOut.ToString());
        }

        [NonEvent]
        public void RequestException(ServerInfo server, Exception ex)
        {
            this.QueryFinished(server.ToString(), RequestStatus.RequestException.ToString(), 0, ex.ToString());
        }


        [NonEvent]
        public void ServerFailureResponse(ServerInfo server, HttpResponseMessage responseMessage)
        {
            this.QueryFinished(server.ToString(), RequestStatus.ServerFailureResponse.ToString(), (int)responseMessage.StatusCode, responseMessage.ReasonPhrase);
        }
    }
}
