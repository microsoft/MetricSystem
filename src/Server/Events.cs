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
    using System.Collections.Generic;
    using System.Diagnostics.Tracing;
    using System.Linq;
    using System.Net;

    [EventSource(Name = "MetricSystem-Server", Guid = "{add7bf8a-b428-4e3f-9058-0226a570a42f}")]
    internal sealed class Events : EventSource
    {
        public static Events Write = new Events();

        [Event(100, Level = EventLevel.Warning)]
        public void RegistrationDestinationResolutionFailed(string destination, string message)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(100, destination, message);
            }
        }

        [Event(101, Level = EventLevel.Informational)]
        public void RegistrationFailed(string destinationUri, int httpCode, string message)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(101, destinationUri, httpCode, message);
            }
        }

        [Event(102, Level = EventLevel.Verbose)]
        public void RegistrationSucceeded(string destinationUri)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(102, destinationUri);
            }
        }

        [Event(103, Level = EventLevel.Informational)]
        public void BeginProcessingQuery(string tag, string info)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(103, tag, info);
            }
        }

        [Event(104, Level = EventLevel.Informational)]
        public void EndProcessingQuery(string tag, string info, int responseCode)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(104, tag, info, responseCode);
            }
        }

        [Event(105, Level = EventLevel.Informational)]
        public void CounterNotFound(string counterName)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(105, counterName);
            }
        }

        [Event(106, Level = EventLevel.Warning)]
        public void UnhandledExceptionProcessingQuery(string tag, string info, string exceptionMessage)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(106, tag, info, exceptionMessage);
            }
        }

        [NonEvent]
        public void InvalidInputBody(HttpListenerRequest request, Exception ex)
        {
            if (this.IsEnabled())
            {
                this.InvalidInputBody(request.RemoteEndPoint.ToString(), request.RawUrl, ex.GetType().ToString(),
                                      ex.Message);
            }
        }

        [Event(107, Level = EventLevel.Warning)]
        private void InvalidInputBody(string sender, string url, string exceptionType, string exceptionMessage)
        {
            this.WriteEvent(107, sender, url, exceptionType, exceptionMessage);
        }

        [NonEvent]
        public void RequestException(Request request, Exception ex)
        {
            if (this.IsEnabled())
            {
                this.RequestException(request.Requester.ToString(), request.Handler.Prefix, request.Path,
                                      string.Join("&", request.QueryParameters.Select(kvp => kvp.Key + '=' + kvp.Value)),
                                      ex.GetType().ToString(), ex.Message);
            }
        }

        [Event(108, Level = EventLevel.Warning)]
        private void RequestException(string sender, string prefix, string path, string queryParameters, string exceptionType, string exceptionMessage)
        {
            this.WriteEvent(108, sender, prefix, path, queryParameters, exceptionType, exceptionMessage);
        }

        [NonEvent]
        public void HttpListenerException(Exception ex)
        {
            if (this.IsEnabled())
            {
                this.HttpListenerException(ex.GetType().ToString(), ex.Message, ex.StackTrace);
            }
        }

        [Event(109, Level = EventLevel.Warning)]
        private void HttpListenerException(string exceptionType, string exceptionMessage, string exceptionStackTrace)
        {
            this.WriteEvent(109, exceptionType, exceptionMessage, exceptionStackTrace);
        }

        [NonEvent]
        public void BeginHandlingRequest(string prefix, string path, IEnumerable<KeyValuePair<string, string>> queryParameters)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.BeginHandlingRequest(prefix, path,
                                          string.Join("&",
                                                      (from pair in queryParameters select pair.Key + '=' + pair.Value)));
            }
        }

        [Event(110, Level = EventLevel.Verbose)]
        public void BeginHandlingRequest(string prefix, string path, string queryParameters)
        {
            this.WriteEvent(110, prefix, path, queryParameters);
        }

        [Event(111, Level = EventLevel.Verbose)]
        public void EndHandlingRequest(string prefix)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(111, prefix);
            }
        }

        [NonEvent]
        public void InvalidContentTypeProvided(Request request, string contentType)
        {
            if (this.IsEnabled())
            {
                this.InvalidContentTypeProvided(request.Requester.ToString(), request.Handler.Prefix, request.Path,
                                                string.Join("&", request.QueryParameters.Select( kvp => kvp.Key + '=' + kvp.Value)),
                                                contentType);
            }
        }

        [Event(112, Level = EventLevel.Warning)]
        private void InvalidContentTypeProvided(string sender, string prefix, string path, string queryParameters, string contentType)
        {
            this.WriteEvent(112, sender, prefix, path, queryParameters, contentType);
        }

        [NonEvent]
        public void SendingErrorResponse(Request request, int errorCode, string errorMessage)
        {
            if (this.IsEnabled())
            {
                this.SendingErrorResponse(request.Requester.ToString(), request.Handler.Prefix, request.Path,
                                          string.Join("&",
                                                      request.QueryParameters.Select(kvp => kvp.Key + '=' + kvp.Value)),
                                          errorCode, errorMessage);
            }
        }

        [Event(113, Level = EventLevel.Informational)]
        private void SendingErrorResponse(string sender, string prefix, string path, string queryParameters,
                                         int errorCode, string errorMessage)
        {
            this.WriteEvent(113, sender, prefix, path, queryParameters, errorCode, errorMessage);
        }

        [Event(200, Level = EventLevel.Informational)]
        public void AddedRemoteHost(string hostname, ushort port)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(200, hostname, port);
            }
        }

        [Event(201, Level = EventLevel.Informational)]
        public void RemovedRemoteHost(string hostname)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(201, hostname);
            }
        }

        [NonEvent]
        public void ExpiringRemoteHosts(DateTimeOffset timestamp)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.ExpiringRemoteHosts(timestamp.ToString());
            }
        }

        [Event(202, Level = EventLevel.Verbose)]
        private void ExpiringRemoteHosts(string timestamp)
        {
            this.WriteEvent(202, timestamp);
        }

        [Event(203, Level = EventLevel.Error)]
        public void CouldNotWriteResponse(string errorMessage)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(203, errorMessage);
            }
        }

        [Event(204, Level = EventLevel.Error)]
        public void RegistryError(string errorMessage, string clientIp)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(204, errorMessage, clientIp);
            }
        }

        [Event(300, Level = EventLevel.Warning)]
        public void ServerLatestDataNowEarlier(string server, string counter, string previousTimestamp,
                                                  string currentTimestamp)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(300, server, counter, previousTimestamp, currentTimestamp);
            }
        }

        [NonEvent]
        public void ServerLatestDataUpdated(string server, string counter, DateTimeOffset newTimestamp)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.ServerLatestDataUpdated(server, counter, newTimestamp.ToString("o"));
            }
        }

        [Event(301, Level = EventLevel.Verbose)]
        private void ServerLatestDataUpdated(string server, string counter, string newTimestamp)
        {
            this.WriteEvent(301, server, counter, newTimestamp);
        }

        [Event(302, Level = EventLevel.Informational)]
        public void ErrorRetrievingServerLatestDataTimestamp(string server, string counter, string message)
        {
            if (this.IsEnabled())
            {
                this.WriteEvent(302, server, counter, message);
            }
        }

        [NonEvent]
        public void BeginRetrieveCounterData(string counterName, DateTimeOffset startTime, DateTimeOffset endTime,
                                             TimeSpan timeout, IList<string> sources)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.BeginRetrieveCounterData(counterName, startTime.ToString("o"), endTime.ToString("o"),
                                              timeout.TotalSeconds, sources.Count);
            }
        }

        [Event(400, Level = EventLevel.Verbose)]
        public void BeginRetrieveCounterData(string counterName, string startTime, string endTime, double timeoutSeconds,
                                             int sourceCount)
        {
            this.WriteEvent(400, counterName, startTime, endTime, timeoutSeconds, sourceCount);
        }

        [NonEvent]
        public void EnqueuedCounterRetrieval(DateTimeOffset timeSlot, string counterName, IEnumerable<string> sources)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.EnqueuedCounterRetrieval(timeSlot.ToString("o"), counterName, string.Join(", ", sources));
            }
        }

        [Event(401, Level = EventLevel.Verbose)]
        public void EndRetrieveCounterData(string counterName, bool success)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(401, counterName, success);
            }
        }

        [Event(402, Level = EventLevel.Verbose)]
        public void EnqueuedCounterRetrieval(string timeSlot, string counterName, string sources)
        {
            this.WriteEvent(402, timeSlot, counterName, sources);
        }

        [Event(403, Level = EventLevel.Verbose)]
        public void BeginScheduleAggregationWork()
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(403);
            }
        }

        [Event(404, Level = EventLevel.Verbose)]
        public void EndScheduleAggregationWork()
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(404);
            }
        }

        [Event(405, Level = EventLevel.Verbose)]
        public void NotAggregatingUnknownCounter(string counterName)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(405, counterName);
            }
        }


        [Event(406, Level = EventLevel.Verbose)]
        public void CreatingCounterAggregator(string counterName)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(406, counterName);
            }
        }

        [Event(407, Level = EventLevel.Verbose)]
        public void BeginSendResponse(string prefix)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(407, prefix);
            }
        }

        [Event(408, Level = EventLevel.Verbose)]
        public void CompleteSendResponse(string prefix)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(408, prefix);
            }
        }

        [Event(409, Level = EventLevel.Verbose)]
        public void BeginCompressingResponse(int responseLength)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(409, responseLength);
            }
        }

        [Event(410, Level = EventLevel.Verbose)]
        public void CompleteCompressingResponse(long compressedLength)
        {
            if (this.IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                this.WriteEvent(410, compressedLength);
            }
        }
    }
}
