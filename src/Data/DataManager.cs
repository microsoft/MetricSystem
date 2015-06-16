// ---------------------------------------------------------------------
// <copyright file="DataManager.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.IO;
    using MetricSystem.Utilities;

    /// <summary>
    /// Delegate called when heap cleanup is desirable.
    /// </summary>
    /// <param name="dataWasReleased">True if the cleaner released some data itself.</param>
    /// <param name="percentOfMax">Percentage of maximum memory currently in use.</param>
    /// <returns>True if a GC should be performed.</returns>
    public delegate bool HandleHeapCleanupRequest(bool dataWasReleased, double percentOfMax);

    /// <summary>
    /// Manager for a collection of counter data.
    /// </summary>
    /// <remarks>
    /// Operations against the top level mapping are NOT thread safe. You should create all hit counters / histograms
    /// prior to any other operations. All other operations against the data are thread safe.
    /// </remarks>
    public sealed class DataManager : IDisposable, ISharedDataSetProperties
    {
        private TimeSpan maintenanceInterval = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Default time after which currently loaded data is sealed and can no longer be modified.
        /// </summary>
        public static readonly TimeSpan DefaultSealTime = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Default time after which data will be removed from memory and deleted from persistent storage.
        /// </summary>
        public static readonly TimeSpan DefaultMaximumDataAge = TimeSpan.FromDays(28);

        private readonly List<Counter> counters = new List<Counter>();
        private Dictionary<string, Counter> counterMap = new Dictionary<string, Counter>();

        private readonly ulong maximumPrivateMemorySizeMB;
        private readonly ulong preferredPrivateMemorySizeMB;
        private readonly ManualResetEventSlim shutdownEvent;
        private SemaphoreTaskRunner taskRunner;

        private Timer maintenanceTimer;
        private DataCompactionConfiguration compactionConfiguration;
        private DateTimeOffset lastDataSetCompaction = DateTimeOffset.MinValue;

        private TimeSpan maximumDataAge;
        private TimeSpan sealTime;

        private Process currentProcess;

        /// <summary>
        /// Value indicating whether there is a pending Heap Cleanup task (1) or not (0). 
        /// Accessed via Interlocked.CompareExchange to act as a synchronization mechanism
        /// </summary>
        private int isCleanupPending = 0;

        public DataManager() : this(null, null, 0, 0, 0) { }

        /// <summary>
        /// Ctor.
        /// </summary>
        /// <param name="storagePath">Base path to store persisted data. If null data will not be written to disk.</param>
        /// <param name="shutdownEvent">Event to trigger when shutdown is desired. If null no event is triggered.</param>
        /// <param name="preferredPrivateMemorySizeMB">Preferred working set size for the current process in MB. Above this size data will be released and garbage will be collected.</param>
        /// <param name="maximumPrivateMemorySizeMB">Maximum working set size for the current process in MB. Above this size the shutdownEvent will be triggered.</param>
        /// <param name="maintenanceParallelismLimit">Maximum parallel threads for periodic data maintenance (e.g. compaction).</param>
        public DataManager(string storagePath, ManualResetEventSlim shutdownEvent, ulong preferredPrivateMemorySizeMB, 
                           ulong maximumPrivateMemorySizeMB, int maintenanceParallelismLimit)
        {
            // just in case our hosting process has not force-compiled the schemas, we do so here as a catch-all
            Schemas.Precompile();

            if (!string.IsNullOrEmpty(storagePath))
            {
                if (!Path.IsPathRooted(storagePath))
                {
                    throw new ArgumentException("Path must be rooted", "storagePath");
                }

                if (!Directory.Exists(storagePath))
                {
                    Directory.CreateDirectory(storagePath);
                }
            }

            if (maintenanceParallelismLimit <= 0)
            {
                maintenanceParallelismLimit = Environment.ProcessorCount;
            }

            this.shutdownEvent = shutdownEvent;
            this.StoragePath = storagePath;

            if (maximumPrivateMemorySizeMB > 0)
            {
                if (shutdownEvent == null)
                {
                    throw new ArgumentNullException("shutdownEvent",
                                                    "Cannot set max working set size without shutdown event.");
                }

                if (maximumPrivateMemorySizeMB < preferredPrivateMemorySizeMB)
                {
                    throw new ArgumentException("Maximum working set size is smaller than preferred.",
                                                "maximumPrivateMemorySizeMB");
                }

                if (preferredPrivateMemorySizeMB == 0)
                {
                    preferredPrivateMemorySizeMB = maximumPrivateMemorySizeMB;
                }

                this.maximumPrivateMemorySizeMB = maximumPrivateMemorySizeMB;
                this.preferredPrivateMemorySizeMB = preferredPrivateMemorySizeMB;
            }
            else if (preferredPrivateMemorySizeMB > 0)
            {
                throw new ArgumentException("Cannot set preferred working set size without max", "preferredPrivateMemorySizeMB");
            }

            this.CompactionConfiguration = new DataCompactionConfiguration();
            this.SealTime = DefaultSealTime;
            this.MaximumDataAge = DefaultMaximumDataAge;
            this.taskRunner = new SemaphoreTaskRunner(maintenanceParallelismLimit);

            // This is a really ridiculous hack to get the full machine name of the current system. Sigh.
            this.LocalSourceName = Dns.GetHostEntry("localhost").HostName;
            //this.CompactionConfiguration = new DataCompactionConfig();

            this.maintenanceTimer = new Timer(_ => this.PeriodicMaintenance(), null, TimeSpan.Zero,
                                              this.MaintenanceInterval);
            // TODO: configurable, for now:
            // 128KB blocks, 1MB large buffer multiples, 128MB max stream
            this.MemoryStreamManager = new RecyclableMemoryStreamManager(1 << 17, 1 << 20, 1 << 27);
            this.MemoryStreamManager.AggressiveBufferReturn = true;
            // If we got a memory limit put a cap on free data
            if (this.preferredPrivateMemorySizeMB > 4)
            {
                var memoryPerPool = (long)this.preferredPrivateMemorySizeMB / 2;
                this.MemoryStreamManager.MaximumFreeLargePoolBytes = 
                    this.MemoryStreamManager.MaximumFreeSmallPoolBytes = memoryPerPool / 2;
            }
        }

        /// <summary>
        /// Retrieve the names of all created data.
        /// </summary>
        /// <returns>An enumerator of names.</returns>
        public IEnumerable<string> CounterNames
        {
            get { return this.counterMap.Keys.ToList(); }
        }

        /// <summary>
        /// Enumeration of all known counters.
        /// </summary>
        public IEnumerable<Counter> Counters
        {
            get { return this.counterMap.Values.ToList(); }
        }

        public TimeSpan MaintenanceInterval
        {
            get { return this.maintenanceInterval; }
            set
            {
                this.maintenanceInterval = value;
                this.maintenanceTimer.Change(TimeSpan.Zero, this.maintenanceInterval);
            }
        }

        #region ISharedDataSetProperties
        public string StoragePath { get; private set; }

        public string LocalSourceName { get; set; }

        /// <remarks>
        /// Changes to this value will not be applied to counters which have already been created.
        /// </remarks>
        public TimeSpan SealTime
        {
            get { return this.sealTime; }
            set
            {
                var newSealTime = this.GetAppropriateSealTime(value);
                if (newSealTime != this.sealTime)
                {
                    Events.Write.SealTimeSet(newSealTime);
                    this.sealTime = newSealTime;
                }
            }
        }

        public TimeSpan MaximumDataAge
        {
            get { return this.maximumDataAge; }
            set
            {
                Events.Write.MaximumAgeSet(value);
                this.maximumDataAge = value;
            }
        }

        public DataCompactionConfiguration CompactionConfiguration
        {
            get { return this.compactionConfiguration; }
            set
            {
                this.compactionConfiguration = value;
                this.SealTime = this.SealTime; // force re-validation of seal time.
            }
        }

        public bool ShuttingDown { get; private set; }
        #endregion

        public RecyclableMemoryStreamManager MemoryStreamManager { get; private set; }

        /// <summary>
        /// Called when heap cleanup is desired, prior to performing a forced generation two collection.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1009:DeclareEventHandlersCorrectly")]
        public event HandleHeapCleanupRequest HeapCleanupDesired;

        public void Dispose()
        {
            lock (this)
            {
                if (this.maintenanceTimer != null)
                {
                    this.maintenanceTimer.Dispose();
                    this.maintenanceTimer = null;
                }

                if (this.currentProcess != null)
                {
                    this.currentProcess.Dispose();
                    this.currentProcess = null;
                }

                foreach (var counter in this.counters)
                {
                    counter.DataSet.Dispose();
                }
                this.counters.Clear();

                if (this.taskRunner != null)
                {
                    this.taskRunner.Dispose();
                    this.taskRunner = null;
                }
            }
        }

        /// <summary>
        /// Find a counter of any type.
        /// </summary>
        /// <param name="name">Name of the counter.</param>
        /// <typeparam name="TCounter">Type of the desired counter.</typeparam>
        /// <returns>A valid TCounter object, or null if the counter could not be found or was of the wrong type.</returns>
        public TCounter GetCounter<TCounter>(string name) where TCounter : class
        {
            if (this.counterMap == null)
            {
                return null;
            }

            Counter counter;
            if (this.counterMap.TryGetValue(name, out counter))
            {
                var castCounter = counter as TCounter;
                if (castCounter != null)
                {
                    return castCounter;
                }

                Events.Write.WrongTypeForCounterRequested(name, typeof(TCounter).ToString(),
                                                          counter.GetType().ToString());
            }
            else
            {
                Events.Write.UnknownDataRequested(name);
            }

            return null;
        }

        /// <summary>
        /// Create a hit counter.
        /// </summary>
        /// <param name="name">Name of the data.</param>
        /// <param name="dimensions">Dimensions for the data.</param>
        [SuppressMessage("Microsoft.Reliability",
            "CA2000:Dispose objects before losing scope")]
        public async Task<HitCounter> CreateHitCounter(string name, DimensionSet dimensions)
        {
            CheckCreateCounter(name, dimensions);
            var path = this.CreateDirectoryForDataSet(name);
            var dataSet =
                new DataSet<InternalHitCount>(name, path, dimensions, this);
            if (path != null)
            {
                await Task.Factory.StartNew(dataSet.LoadStoredData);
            }

            var counter = new HitCounter(dataSet);
            this.AddCounter(counter);
            return counter;
        }

        /// <summary>
        /// Flush all counter data to a queryable state.
        /// </summary>
        public void Flush()
        {
            lock (this.counters)
            {
                foreach (var c in this.counters)
                {
                    c.DataSet.Flush();
                }
            }
        }

        /// <summary>
        /// Create histogram data.
        /// </summary>
        /// <param name="name">Name of the data.</param>
        /// <param name="dimensions">Dimensions for the data.</param>
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope"),
         SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public async Task<HistogramCounter> CreateHistogramCounter(string name, DimensionSet dimensions)
        {
            CheckCreateCounter(name, dimensions);
            var path = this.CreateDirectoryForDataSet(name);
            var dataSet =
                new DataSet<InternalHistogram>(name, path, dimensions, this);

            if (path != null)
            {
                await Task.Factory.StartNew(dataSet.LoadStoredData);
            }

            var counter = new HistogramCounter(dataSet);
            this.AddCounter(counter);
            return counter;
        }

        private void AddCounter(Counter counter)
        {
            lock (this.counters)
            {
                // Safe to do this since only AddCounter can change the list.
                if (this.counterMap.ContainsKey(counter.Name))
                {
                    throw new InvalidOperationException("Attempt to add duplicate counter " + counter.Name);
                }
                this.counters.Add(counter);

                var newMap = new Dictionary<string, Counter>(this.counters.Count, StringComparer.OrdinalIgnoreCase);
                foreach (var c in this.counters)
                {
                    newMap.Add(c.Name, c);
                }
                Interlocked.Exchange(ref this.counterMap, newMap);
            }
        }

        private static void CheckCreateCounter(string name, DimensionSet dimensionSet)
        {
            if (!Protocol.IsValidCounterName(name))
            {
                throw new ArgumentException("Counter name is invalid.", "name");
            }

            if (dimensionSet == null)
            {
                throw new ArgumentNullException("dimensionSet");
            }
        }

        private string CreateDirectoryForDataSet(string name)
        {
            if (this.StoragePath == null)
            {
                return null;
            }

            // The Path class will happily handle either / or \ as path separators for us. However, when combining paths
            // we must ensure the right-hand path does not start with a 'root' path.
            // Valid counter names must start with this character so we take the substring. In addition counter name
            // validation ensures that the name may not contain any other invalid path characters.
            var path = Path.Combine(this.StoragePath, name.Substring(1));
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

            return path;
        }

        private bool HasMemoryExceededLimit(ulong limitMB, out ulong privateMemorySizeMB)
        {
            if (this.currentProcess == null)
            {
                this.currentProcess = Process.GetCurrentProcess();
            }
            else
            {
                this.currentProcess.Refresh();
            }

            privateMemorySizeMB = (ulong)(this.currentProcess.PrivateMemorySize64 / (1024 * 1024));
            return limitMB > 0 && privateMemorySizeMB > limitMB;
        }

        private bool HasMemoryUsageExceededPreferredMaximum(out ulong privateMemorySizeMB)
        {
            return HasMemoryExceededLimit(this.preferredPrivateMemorySizeMB, out privateMemorySizeMB);
        }

        private bool HasMemoryUsageExceededMaximum(out ulong privateMemorySizeMB)
        {
            return HasMemoryExceededLimit(this.maximumPrivateMemorySizeMB, out privateMemorySizeMB);
        }

        private void PeriodicMaintenance()
        {
            Events.Write.BeginPeriodicMaintenance();
            if (this.maximumPrivateMemorySizeMB > 0 && this.shutdownEvent != null)
            {
                // when the maintenance timer goes off, take a minilock (isCleanupPending = 1) to indicate exclusive access to 
                // cleanup. When cleanup is done (later at the end of the task), unlock (isCleanupPending = 0) 
                if (Interlocked.CompareExchange(ref this.isCleanupPending, 1, 0) == 0)
                {
                    Task.Factory.StartNew(() =>
                                          {
                                              this.CleanupHeapIfNeeded();
                                              Interlocked.Exchange(ref this.isCleanupPending, 0);
                                          });
                }
            }

            if (DateTimeOffset.UtcNow - this.lastDataSetCompaction >= DataCompactionConfiguration.MinimumIndividualDuration)
            {
                foreach (var counter in this.Counters)
                {
                    this.taskRunner.Schedule(counter.DataSet.Compact);
                }

                this.lastDataSetCompaction = DateTimeOffset.UtcNow;
            }

            Events.Write.EndPeriodicMaintenance();
        }

        // We intentionally run a routine to unlink large tables of data, the collection is meant to pull them out of
        // memory proactively.
        [SuppressMessage("Microsoft.Reliability", "CA2001:AvoidCallingProblematicMethods",
            MessageId = "System.GC.Collect")]
        private void CleanupHeapIfNeeded()
        {
            ulong privateMemorySize;
            bool shouldGC = false;
            if (this.HasMemoryUsageExceededPreferredMaximum(out privateMemorySize))
            {
                Events.Write.BeginHeapCleanup(privateMemorySize, this.preferredPrivateMemorySizeMB, this.maximumPrivateMemorySizeMB);
                foreach (var counter in this.counterMap.Values)
                {
                    shouldGC |= counter.DataSet.ReleaseOldestData(false);
                }

                var percentOfMax = privateMemorySize / (double)this.maximumPrivateMemorySizeMB;

                // Allow heap cleanup handlers to suggest cleanup as well, hinting at whether we performed any ourselves.
                if (this.HeapCleanupDesired != null)
                {
                    shouldGC |= this.HeapCleanupDesired(shouldGC, percentOfMax);
                }

                if (shouldGC)
                {
                    Events.Write.BeginPerformGarbageCollection();
                    GC.Collect(2, GCCollectionMode.Forced);
                    Events.Write.EndPerformGarbageCollection();
                }

                // Still not gone below target limit => restart
                if (this.shutdownEvent != null && this.HasMemoryUsageExceededMaximum(out privateMemorySize))
                {
                    Events.Write.ShuttingDownDueToHighMemoryUsage(privateMemorySize, this.maximumPrivateMemorySizeMB);
                    this.Shutdown();
                }

                Events.Write.EndHeapCleanup(privateMemorySize);
            }
        }

        // Ensures that when a user asks for a seal time it is always no larger than the seal time of the default
        // (smallest) bucket size. Also converts negative values to 'zero'. If a user asks for no sealing but
        // we have multiple bucket sizes we force sealing after the minimum duration of storage for the first bucket.
        private TimeSpan GetAppropriateSealTime(TimeSpan desiredSealTime)
        {
            desiredSealTime = desiredSealTime < TimeSpan.Zero ? TimeSpan.Zero : desiredSealTime;

            var newSealTime = this.CompactionConfiguration.Default.Duration;
            if (desiredSealTime > TimeSpan.Zero && desiredSealTime < newSealTime)
            {
                newSealTime = desiredSealTime;
            }

            return newSealTime;
        }

        private void Shutdown()
        {
            this.ShuttingDown = true;
            if (this.shutdownEvent != null)
            {
                this.shutdownEvent.Set();
            }
        }
    }
}
