// ---------------------------------------------------------------------
// <copyright file="PerformanceTests.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime;
    using System.Threading.Tasks;

    using NUnit.Framework;

    [TestFixture]
    public sealed class PerformanceTests
    {
        private const string StoragePath = "performanceTests";

        private DataManager dataManager;

        [SetUp]
        public void SetUp()
        {
            var storagePath = Path.Combine(Directory.GetCurrentDirectory(), StoragePath);
            if (Directory.Exists(storagePath))
            {
                Directory.Delete(storagePath, true);
            }

            this.dataManager = new DataManager(storagePath, null, 0, 0, 4);
            this.dataManager.HeapCleanupDesired +=
                (released, percentOfMax) =>
                {
                    if (released)
                    {
                        GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                        return true;
                    }

                    return false;
                };
        }

        [TearDown]
        public void TearDown()
        {
            this.dataManager.Dispose();
        }

        struct SingleCounterConfiguration<TCounter> where TCounter : Counter
        {
            public TCounter Counter;
            public int[] DimensionMaximums;
            public int WritesPerInterval;
            public Action<TCounter, DateTime, DimensionSpecification, long> Writer;
        }

        [Ignore, Category("QTestSkip")]
        [Test]
        public async Task OneMinuteHeavyLoad()
        {
            var start = DateTime.Now;
            var end = start + this.dataManager.CompactionConfiguration.Default.Interval;

            var hitCounters = new List<SingleCounterConfiguration<HitCounter>>();
            var histogramCounters = new List<SingleCounterConfiguration<HistogramCounter>>();

            var twoDimSet = DimensionSetTests.CreateDimensionSet(2);
            var threeDimSet = DimensionSetTests.CreateDimensionSet(3);
            for (var i = 0; i < 10; ++i)
            {
                var counter = new SingleCounterConfiguration<HitCounter>
                                 {
                                     Counter =
                                         await this.dataManager.CreateHitCounter("/HitCounter/TwoDim/" + i, twoDimSet),
                                     DimensionMaximums = new[] {5, 500},
                                     WritesPerInterval = 60 * 10,
                                     Writer = this.WriteHitCount,
                                 };
                hitCounters.Add(counter);
            }
            for (var i = 0; i < 5; ++i)
            {
                var counter = new SingleCounterConfiguration<HistogramCounter>
                              {
                                  Counter =
                                      await
                                      this.dataManager.CreateHistogramCounter("/SmallHistogram/TwoDim/" + i, twoDimSet),
                                  DimensionMaximums = new[] {5, 500},
                                  WritesPerInterval = 60 * 10,
                                  Writer = this.WriteSmallHistogram,
                              };
                histogramCounters.Add(counter);
            }

            for (var i = 0; i < 10; ++i)
            {
                var counter = new SingleCounterConfiguration<HitCounter>
                                 {
                                     Counter =
                                         await this.dataManager.CreateHitCounter("/HitCounter/ThreeDim/" + i, threeDimSet),
                                     DimensionMaximums = new[] {5, 250, 1000},
                                     WritesPerInterval = 60 * 10 * 1000,
                                     Writer = this.WriteHitCount,
                                 };
                hitCounters.Add(counter);
            }
            for (var i = 0; i < 5; ++i)
            {
                var counter = new SingleCounterConfiguration<HistogramCounter>
                              {
                                  Counter =
                                      await
                                      this.dataManager.CreateHistogramCounter("/SmallHistogram/ThreeDim/" + i, threeDimSet),
                                  DimensionMaximums = new[] {5, 250, 1000},
                                  WritesPerInterval = 60 * 10 * 1000,
                                  Writer = this.WriteSmallHistogram,
                              };
                histogramCounters.Add(counter);
            }

            var tasks = new List<Task>();
            foreach (var config in hitCounters)
            {
                var t = new Task(() => this.WriteRandomlyKeyedData(config, start, end));
                t.Start();
                tasks.Add(t);
            }
            foreach (var config in histogramCounters)
            {
                var t = new Task(() => this.WriteRandomlyKeyedData(config, start, end));
                t.Start();
                tasks.Add(t);
            }

            await Task.WhenAll(tasks);

            foreach (var c in hitCounters)
            {
                c.Counter.DataSet.Flush();
                foreach (var d in c.Counter.Query(new DimensionSpecification()))
                {
                    Trace.WriteLine(d);
                }
            }
            foreach (var c in histogramCounters)
            {
                c.Counter.DataSet.Flush();
                foreach (var d in c.Counter.Query(new DimensionSpecification()))
                {
                    Trace.WriteLine(d);
                }
            }
        }

        private void WriteHitCount(HitCounter c, DateTime ts, DimensionSpecification spec, long value)
        {
            c.Increment(value, spec, ts);
        }

        private void WriteSmallHistogram(HistogramCounter c, DateTime ts, DimensionSpecification spec, long value)
        {
            c.AddValue(value, spec, ts);
        }

        private void WriteRandomlyKeyedData<TCounter>(SingleCounterConfiguration<TCounter> config, DateTime start, DateTime end)
            where TCounter : Counter
        {
            var rng = new Random();
            var spec = new DimensionSpecification();
            for (var ts = start;
                 start < end;
                 start += this.dataManager.CompactionConfiguration.Default.Interval)
            {
                for (var i = 0; i < config.WritesPerInterval; ++i)
                {
                    for (var d = 0; d < config.DimensionMaximums.Length; ++d)
                    {
                        spec[d.ToString()] = rng.Next(config.DimensionMaximums[d]).ToString();
                    }

                    config.Writer(config.Counter, ts, spec, rng.Next(1024));
                }
            }
        }
    }
}
