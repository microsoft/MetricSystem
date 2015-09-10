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
    using System.Linq;
    using System.Threading;

    using MetricSystem.Data;

    /// <summary>
    /// maintains a list of server objects registered to the aggregation server.
    /// </summary>
    internal sealed class ServerList : IDisposable, IEnumerable<ServerInfo>
    {
        /// <summary>
        /// Time to wait before expiring servers that have not been updated.
        /// </summary>
        public TimeSpan ExpirationTime { get; set; }

        public event HandleServerCounterTimeUpdate LatestCounterTimeUpdated;
        private readonly Dictionary<string, ServerInfo> serverTable;
        private readonly Timer expireTimer;
        private static readonly TimeSpan ExpireCheckInterval = TimeSpan.FromMinutes(1);

        public ServerList()
        {
            this.serverTable = new Dictionary<string, ServerInfo>(StringComparer.OrdinalIgnoreCase);
            this.expireTimer = new Timer(_ => this.ExpireServers(DateTimeOffset.Now), null, TimeSpan.Zero,
                                         ExpireCheckInterval);
        }

        public IList<MetricSystem.ServerInfo> Servers
        {
            get
            {
                lock (this.serverTable)
                {
                    return (from server in this.serverTable.Values
                            select new MetricSystem.ServerInfo
                                   {
                                       Hostname = server.Hostname,
                                       Port = server.Port,
                                       MachineFunction = server.MachineFunction,
                                       Datacenter = server.Datacenter,
                                   }).ToList();
                }
            }
        }

        internal void ExpireServers(DateTimeOffset now)
        {
            Events.Write.ExpiringRemoteHosts(now);

            var minimumUpdateTime = now - this.ExpirationTime;
            lock (this.serverTable)
            {
                var keys = this.serverTable.Keys.ToList();
                foreach (var k in keys)
                {
                    if (this.serverTable[k].LastUpdateTime < minimumUpdateTime)
                    {
                        this.serverTable.Remove(k);
                    }
                }
            }
        }

        internal ServerInfo this[string hostname]
        {
            get
            {
                lock (this.serverTable)
                {
                    return this.serverTable[hostname];
                }
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability",
            "CA2000:Dispose objects before losing scope")]
        public ServerInfo InsertOrUpdate(ServerRegistration registration)
        {
            var currentTime = DateTimeOffset.Now;

            ServerInfo host;
            lock (this.serverTable)
            {
                if (!this.serverTable.TryGetValue(registration.Hostname, out host))
                {
                    Events.Write.AddedRemoteHost(registration.Hostname, registration.Port);
                    host = new ServerInfo(registration);
                    this.serverTable[registration.Hostname] = host;

                    host.LatestCounterTimeUpdated +=
                        (server, counterName, timestamp) =>
                        {
                            if (this.LatestCounterTimeUpdated != null)
                            {
                                this.LatestCounterTimeUpdated(server, counterName, timestamp);
                            }
                        };
                }
            }
            host.LastUpdateTime = currentTime;

            return host;
        }

        public void Dispose()
        {
            this.expireTimer.Dispose();
        }

        #region IEnumerable<ServerInfo> Members
        public IEnumerator<ServerInfo> GetEnumerator()
        {
            lock (this.serverTable)
            {
                foreach (var s in this.serverTable.Values)
                {
                    yield return s;
                }
            }
        }
        #endregion

        #region IEnumerable Members
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
        #endregion
    }
}
