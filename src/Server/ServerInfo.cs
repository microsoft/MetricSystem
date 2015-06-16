// ---------------------------------------------------------------------
// <copyright file="ServerInfo.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
