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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using MetricSystem.Data;
    using MetricSystem.Utilities;

    /// <summary>
    /// This class is the work scheduler for aggregating data from downstream servers.
    /// </summary>
    internal sealed class AggregationPoller : IDisposable
    {
        private static readonly Comparer<DateTimeOffset> ReverseDateTimeOffsetComparer =
            Comparer<DateTimeOffset>.Create((x, y) => y.CompareTo(x));

        private readonly DataManager dataManager;
        private readonly TimeSpan pollingInterval;

        private readonly HashSet<string> activeWorkers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, DateTimeOffset>> serverTimestamps =
            new ConcurrentDictionary<string, ConcurrentDictionary<string, DateTimeOffset>>(
                StringComparer.OrdinalIgnoreCase);

        private Timer pollTimer;

        // Timeout tiers based on number of servers.
        private const int MaxFanout = 20;
        private static readonly int[] ServerCountTiers =
        {
            MaxFanout,
            MaxFanout * MaxFanout,
            (int)Math.Pow(MaxFanout, 3),
            (int)Math.Pow(MaxFanout, 4)
        };

        private static readonly TimeSpan[] TimeoutTiers =
        {
            TimeSpan.FromSeconds(5),
            TimeSpan.FromSeconds(12),
            TimeSpan.FromSeconds(20),
            TimeSpan.FromSeconds(30),
        };

        //private static readonly int MaxWorkers = Environment.ProcessorCount; // TODO: configurable.
        //private const int MaxWorkers = 8; // TODO: configurable.
        private SemaphoreTaskRunner taskRunner;
        private bool disposed = false;

        public AggregationPoller(ServerList serverList, DataManager dataManager, TimeSpan pollingInterval)
        {
            if (serverList == null)
            {
                throw new ArgumentNullException("serverList");
            }

            if (dataManager == null)
            {
                throw new ArgumentNullException("dataManager");
            }

            if (pollingInterval <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException("pollingInterval");
            }

            serverList.LatestCounterTimeUpdated += this.UpdateServerCounterTime;
            this.dataManager = dataManager;
            this.pollingInterval = pollingInterval;
            this.taskRunner = new SemaphoreTaskRunner(this.dataManager.Counters.Count());
        }

        public void Dispose()
        {
            lock (this)
            {
                if (this.disposed)
                {
                    return;
                }

                this.disposed = true;

                if (this.pollTimer != null)
                {
                    this.pollTimer.Dispose();
                    this.pollTimer = null;
                }

                if (this.taskRunner != null)
                {
                    this.taskRunner.Dispose();
                    this.taskRunner = null;
                }
            }
        }

        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public void Start()
        {
            if (this.pollTimer != null)
            {
                throw new InvalidOperationException("Already started polling.");
            }

            this.pollTimer = new Timer(_ => this.ScheduleUpdateWork(), null, TimeSpan.Zero, this.pollingInterval);
        }

        private void UpdateServerCounterTime(ServerInfo server, string counterName,
                                             DateTimeOffset timestamp)
        {
            var counterData = this.serverTimestamps.GetOrAdd(counterName,
                                                             key =>
                                                             new ConcurrentDictionary<string, DateTimeOffset>(
                                                                 StringComparer.OrdinalIgnoreCase));
            counterData.AddOrUpdate(server.Hostname, timestamp, (key, value) =>
                                                                (value > timestamp ? value : timestamp));
        }

        [SuppressMessage("Microsoft.Performance", "CA1804:RemoveUnusedLocals", MessageId = "t",
            Justification = "Using this pattern to 'fire and forget' a task"),
         SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private void ScheduleUpdateWork()
        {
            Events.Write.BeginScheduleAggregationWork();

            lock (this)
            {
                if (this.disposed)
                {
                    return;
                }

                foreach (var counterData in this.serverTimestamps)
                {
                    var counter = this.dataManager.GetCounter<Counter>(counterData.Key);
                    if (counter == null)
                    {
                        Events.Write.NotAggregatingUnknownCounter(counterData.Key);
                        continue;
                    }
                    if (!counter.ShouldAggregate())
                    {
                        continue;
                    }

                    lock (this.activeWorkers)
                    {
                        if (this.activeWorkers.Contains(counter.Name))
                        {
                            continue;
                        }

                        Events.Write.CreatingCounterAggregator(counter.Name);
                        this.activeWorkers.Add(counter.Name);
                    }

                    var t = this.ScheduleWorker(counter);
                }
            }

            Events.Write.EndScheduleAggregationWork();
        }

        private async Task ScheduleWorker(Counter counter)
        {
            await this.taskRunner.RunAsync(() => this.GetPendingCounterData(counter));
        }

        private void UpdateCounterWithLatestTimestamps(Counter counter)
        {
            var counterData = this.serverTimestamps[counter.Name];
            var serversByTimestamp = new SortedList<DateTimeOffset, List<string>>(ReverseDateTimeOffsetComparer);
            foreach (var kvp in counterData)
            {
                var serverName = kvp.Key;
                var serverTimestamp = kvp.Value;
                List<string> serverList;
                if (!serversByTimestamp.TryGetValue(serverTimestamp, out serverList))
                {
                    serverList = new List<string>();
                    serversByTimestamp.Add(serverTimestamp, serverList);
                }

                serverList.Add(serverName);
            }
            counter.DataSet.SetLatestTimeForDataSources(serversByTimestamp);
        }

        private async Task GetPendingCounterData(Counter counter)
        {
            this.UpdateCounterWithLatestTimestamps(counter);

            DateTimeOffset lastPending = DateTimeOffset.MinValue;
            PendingData pendingData = counter.DataSet.GetNextPendingData(lastPending);
            if (pendingData == null)
            {
                return;
            }

            do
            {
                if (this.disposed)
                {
                    return;
                }

                lastPending = pendingData.StartTime;

                // TODO: Move below method to DateTimeOffset too.
                using (var aggregator =
                    PersistedDataProtocol.CreateAggregatorForSampleType((MetricSystem.PersistedDataType)counter.DataSet.PersistedDataType,
                                                                        counter.Name,
                                                                        counter.DataSet.DimensionSet,
                                                                        pendingData.Sources,
                                                                        new DateTime(
                                                                            pendingData.StartTime.DateTime.Ticks,
                                                                            DateTimeKind.Utc),
                                                                        new DateTime(pendingData.EndTime.DateTime.Ticks,
                                                                                     DateTimeKind.Utc),
                                                                        this.dataManager.MemoryStreamManager))
                {
                    var timeout = TimeoutTiers[ServerCountTiers.Length - 1];
                    for (var i = 0; i < ServerCountTiers.Length; ++i)
                    {
                        if (pendingData.Sources.Count <= ServerCountTiers[i])
                        {
                            timeout = TimeoutTiers[i];
                            break;
                        }
                    }
                    aggregator.MaxFanout = MaxFanout;
                    aggregator.Timeout = timeout;

                    Events.Write.BeginRetrieveCounterData(counter.Name, pendingData.StartTime, pendingData.EndTime,
                                                          timeout, pendingData.Sources);
                    var success = await aggregator.Run();
                    if (success)
                    {
                        counter.DataSet.UpdateFromAggregator(aggregator, pendingData.StartTime, pendingData.EndTime);
                    }

                    Events.Write.EndRetrieveCounterData(counter.Name, success);
                }

                this.UpdateCounterWithLatestTimestamps(counter);
            } while ((pendingData = counter.DataSet.GetNextPendingData(lastPending)) != null);

            lock (this.activeWorkers)
            {
                this.activeWorkers.Remove(counter.Name);
            }
        }
    }
}
