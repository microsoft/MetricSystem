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
    using System.Collections.Concurrent;

    using MetricSystem.Utilities;

    internal delegate void HandleServerCounterTimeUpdate(
        ServerInfo server, string counterName, DateTimeOffset latestTime);

    internal sealed class ServerInfo
    {
        private DateTimeOffset lastUpdateTime;

        public ServerInfo(ServerRegistration registration)
        {
            if (string.IsNullOrEmpty(registration.Hostname))
            {
                throw new ArgumentException("No hostname provided.", "registration.Hostname");
            }
            if (registration.Port == 0)
            {
                throw new ArgumentOutOfRangeException("registration.Port");
            }

            this.Hostname = registration.Hostname;
            this.Port = registration.Port;
            this.MachineFunction = registration.MachineFunction;
            this.Datacenter = registration.Datacenter;
            this.lastUpdateTime = DateTimeOffset.MinValue;
            this.CounterInfo = new ConcurrentDictionary<CounterInfo, DateTimeOffset>();
        }

        public ConcurrentDictionary<CounterInfo, DateTimeOffset> CounterInfo { get; private set; }
        public string Hostname { get; private set; }
        public ushort Port { get; private set; }
        public string MachineFunction { get; private set; }
        public string Datacenter { get; private set; }

        public DateTimeOffset LastUpdateTime
        {
            get { return this.lastUpdateTime; }
            set
            {
                if (this.lastUpdateTime < value)
                {
                    this.lastUpdateTime = value;
                }
            }
        }

        public event HandleServerCounterTimeUpdate LatestCounterTimeUpdated;

        public void ProcessRegistrationMessage(ServerRegistration message)
        {
            // These can change over time so go ahead and update.
            this.Port = message.Port;
            this.MachineFunction = message.MachineFunction;
            this.Datacenter = message.Datacenter;

            foreach (var counter in message.Counters)
            {
                var updated = false;
                var endTime = counter.EndTime.ToDateTimeOffset();
                if (endTime == DateTimeOffset.MinValue)
                {
                    continue;
                }

                this.CounterInfo.AddOrUpdate(counter,
                                             info =>
                                             {
                                                 updated = true;
                                                 return endTime;
                                             },
                                             (info, current) =>
                                             {
                                                 if (endTime > current)
                                                 {
                                                     updated = true;
                                                     return endTime;
                                                 }

                                                 if (endTime < current)
                                                 {
                                                     Events.Write.ServerLatestDataNowEarlier(this.Hostname, info.Name,
                                                                                             current.ToString("o"),
                                                                                             endTime.ToString("o"));
                                                 }
                                                 return current;
                                             });

                if (updated)
                {
                    if (this.LatestCounterTimeUpdated != null)
                    {
                        this.LatestCounterTimeUpdated(this, counter.Name, endTime);
                    }

                    Events.Write.ServerLatestDataUpdated(this.Hostname, counter.Name, endTime);
                }
            }
        }

        public override string ToString()
        {
            return string.Format("{0}:{1}: {2:o}", this.Hostname, this.Port, this.LastUpdateTime);
        }
    }
}
