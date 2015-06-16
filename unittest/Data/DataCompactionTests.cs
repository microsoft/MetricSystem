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

namespace MetricSystem.Data.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    using Newtonsoft.Json;

    using NUnit.Framework;

    [TestFixture]
    public sealed class DataCompactionTests
    {
        private DataSet<InternalHitCount> dataSet;

        private const string CounterName = "testCounter";
        private string storagePath;
        private MockSharedDataSetProperties properties;

        private readonly DataCompactionConfiguration customCompaction
            = new DataCompactionConfiguration(new List<DataIntervalConfiguration>
                                       {
                                           new DataIntervalConfiguration(TimeSpan.FromMinutes(1), TimeSpan.FromHours(1)),
                                           new DataIntervalConfiguration(TimeSpan.FromMinutes(5), TimeSpan.FromHours(1)),
                                           new DataIntervalConfiguration(TimeSpan.FromMinutes(10), TimeSpan.MaxValue)
                                       });

        private DimensionSet dimensionSet;
        private DimensionSpecification dimensions;
        private const string DimValue = "dv";

        [SetUp]
        public void SetUp()
        {
            this.dimensionSet =
                new DimensionSet(new HashSet<Dimension>
                                 {
                                     new Dimension("foo"),
                                     new Dimension("bar"),
                                     new Dimension("baz"),
                                 });

            dimensions = new DimensionSpecification();
            this.storagePath = Path.Combine(Environment.CurrentDirectory, "msTemp");
            if (Directory.Exists(this.storagePath))
            {
                Directory.Delete(this.storagePath, true);
            }

            Directory.CreateDirectory(this.storagePath);
            foreach (var d in this.dimensionSet.Dimensions)
            {
                dimensions[d.Name] = DimValue;
            }

            this.properties = new MockSharedDataSetProperties();
        }

        [TearDown]
        public void TearDown()
        {
            if (this.dataSet != null)
            {
                this.dataSet.Dispose();
                this.dataSet = null;
            }

            if (Directory.Exists(this.storagePath))
            {
                Directory.Delete(this.storagePath, true);
            }
        }


        private void InitializeDataSet(DataCompactionConfiguration compactionConfig)
        {
            compactionConfig = compactionConfig ?? this.customCompaction;
            var mockSharedProperties = new MockSharedDataSetProperties
                                       {
                                           CompactionConfiguration = compactionConfig,
                                           SealTime = compactionConfig.Default.Interval,
                                       };
            this.dataSet =
                new DataSet<InternalHitCount>(CounterName, storagePath, this.dimensionSet,
                                                                         mockSharedProperties);
            dataSet.LoadStoredData();
        }

        [Test]
        public void BucketsAreCreatedWithDefaultCompactionTicks()
        {
            this.InitializeDataSet(null);
            var timeStamp = DateTime.Now;
            var hitCounter = new HitCounter(this.dataSet);
            hitCounter.Increment(3, this.dimensions, timeStamp);
            var bucket = this.dataSet.GetDataBucket(timeStamp.ToUniversalTime());
            Assert.AreEqual(bucket.EndTicks - bucket.StartTicks, this.customCompaction.DefaultBucketTicks);
        }

        [Test]
        public void ExistingDataFilesAreCompactedOnCompactCall()
        {
            // Create one bucket at present time so that remaining buckets
            // being created are out of compaction
            var timeStamp = DateTime.UtcNow;
            var bucket =
                new DataBucket<InternalHitCount>(this.dimensionSet, timeStamp, TimeSpan.TicksPerMinute,
                                                                            this.storagePath,
                                                                            this.properties.MemoryStreamManager);
            bucket.AddValue(this.dimensions, 5);
            bucket.Seal();
            bucket.Persist();

            // Create buckets manually at 1 min and 5 min quantum
            // but they actually belong at a 10 min once compacted
            var expectedNewBucketQuantum = customCompaction.Intervals.ElementAt(2).Interval.Ticks;
            var timeStamp2 = RoundTimeStamp(timeStamp.Subtract(new TimeSpan(2, 20, 0)), expectedNewBucketQuantum);

            // The buckets will have files written out but not be 
            // part of the dataset until it is created
            var bucket1 =
                new DataBucket<InternalHitCount>(this.dimensionSet, timeStamp2, TimeSpan.TicksPerMinute,
                                                                            this.storagePath,
                                                                            this.properties.MemoryStreamManager);
            bucket1.AddValue(this.dimensions, 5);
            bucket1.Seal();
            bucket1.Persist();
            var timeStamp3 = timeStamp2.Add(new TimeSpan(0, 5, 0));
            var bucket2 =
                new DataBucket<InternalHitCount>(this.dimensionSet, timeStamp3,
                                                                            TimeSpan.TicksPerMinute * 5,
                                                                            this.storagePath,
                                                                            this.properties.MemoryStreamManager);
            bucket2.AddValue(this.dimensions, 2);
            bucket2.Seal();
            bucket2.Persist();

            bucket.Dispose();
            bucket1.Dispose();
            bucket2.Dispose();

            this.InitializeDataSet(null);
            this.dataSet.Compact();

            // Verify that a 10 minute compacted bucket was created
            var newBucket = this.dataSet.GetDataBucket(timeStamp2);
            Assert.AreEqual(timeStamp2.Ticks, newBucket.StartTicks);
            Assert.AreEqual(expectedNewBucketQuantum, newBucket.EndTicks - newBucket.StartTicks);
            var matches = newBucket.GetMatches(this.dimensions).ToList();
            Assert.AreEqual(1, matches.Count);
            var result = matches[0].Data;
            Assert.AreEqual((ulong)7, result.HitCount);
        }

        [Test]
        public void HistogramCompactionIsCorrect()
        {
            const int numBuckets = 5;
            const int samplesPerBucket = 100;
            var buckets = new DataBucket<InternalHistogram>[numBuckets];

            for (int i = 0; i < numBuckets; i++)
            {
                var newBucket =
                    new DataBucket<InternalHistogram>(this.dimensionSet, DateTime.Now,
                                                                                          TimeSpan.FromMinutes(1).Ticks,
                                                                                          this.storagePath,
                                                                                          this.properties.MemoryStreamManager);
                buckets[i] = newBucket;

                for (int sample = 0; sample < samplesPerBucket; sample++)
                {
                    newBucket.AddValue(this.dimensions, sample);
                }
                newBucket.Seal();
            }

            var compactedBucket =
                new DataBucket<InternalHistogram>(buckets, this.dimensionSet, DateTime.Now,
                                                                                      TimeSpan.FromMinutes(60).Ticks,
                                                                                      this.storagePath,
                                                                                      this.properties.MemoryStreamManager);

            var data = compactedBucket.GetMatches(this.dimensions);
            var singleVal = data.First().Data;
            Assert.IsNotNull(singleVal);
            Assert.AreEqual(numBuckets * samplesPerBucket, (int)singleVal.SampleCount);
            compactedBucket.Dispose();
        }

        [Test]
        public void MultipleCompactionLevelsAreHonored()
        {
            this.InitializeDataSet(
                new DataCompactionConfiguration(
                    new []
                    {
                        new DataIntervalConfiguration(TimeSpan.FromMinutes(1), TimeSpan.FromHours(2)), 
                        new DataIntervalConfiguration(TimeSpan.FromMinutes(5), TimeSpan.FromHours(4)), 
                        new DataIntervalConfiguration(TimeSpan.FromMinutes(10), TimeSpan.FromHours(6)), 
                        new DataIntervalConfiguration(TimeSpan.FromHours(1), TimeSpan.MaxValue), 
                    }));

            const int expectedBucketCount = 60 * 2 + 12 * 4 + 6 * 6 + 46;

            var startTime = DateTimeOffset.Parse("2012/05/04 00:00:00");
            var span = TimeSpan.FromDays(2);
            var defaultTicks = customCompaction.DefaultBucketTicks;

            for (int i = 0; i < span.Ticks / defaultTicks; ++i)
            {
                var timestamp = (startTime + TimeSpan.FromTicks(i * defaultTicks)).UtcDateTime;
                this.dataSet.AddValue(867, this.dimensions, timestamp);
            }

            this.dataSet.Compact();
            var times = this.dataSet.GetDimensionValues(ReservedDimensions.StartTimeDimension,
                                                        new DimensionSpecification()).ToList();
            Assert.AreEqual(expectedBucketCount, times.Count);
        }

        [Test]
        public void GetEarliestTimestampsPerBucketProducesCorrectSegments()
        {
            var config =
                new DataCompactionConfiguration(new[]
                                         {
                                             new DataIntervalConfiguration(TimeSpan.FromMinutes(1),
                                                                  TimeSpan.FromHours(2)),
                                             new DataIntervalConfiguration(TimeSpan.FromMinutes(5),
                                                                  TimeSpan.FromHours(46)),
                                             new DataIntervalConfiguration(TimeSpan.FromMinutes(10),
                                                                  TimeSpan.FromDays(12)),
                                             new DataIntervalConfiguration(TimeSpan.FromHours(1), TimeSpan.MaxValue),
                                         });

            // Take a last time of ~2014/07/04 18:35:00
            // Expect: 2+ hrs of one minute buckets (from 2014/07/04 16:35 forward)
            //         46 hrs of 5 minute buckets (from 2014/07/02 16:30 forward)
            //         12 days of 10 minute buckets (from 2014/06/22 16:30 forward)
            var firstBucketTime = new DateTime(2014, 7, 4, 16, 35, 0, DateTimeKind.Utc);
            var secondBucketTime = new DateTime(2014, 7, 2, 18, 30, 0, DateTimeKind.Utc);
            var thirdBucketTime = new DateTime(2014, 6, 20, 18, 0, 0, DateTimeKind.Utc);

            foreach (var latestTime in new[]
                                       {
                                           new DateTime(2014, 7, 4, 18, 35, 0, DateTimeKind.Utc),
                                           new DateTime(2014, 7, 4, 18, 38, 21, DateTimeKind.Utc),
                                       })
            {
                var stamps = config.GetEarliestTimestampsPerBucket(latestTime);

                Assert.AreEqual(config.Intervals.Count, stamps.Count);
                Assert.AreEqual(firstBucketTime, stamps.Keys[0]);
                Assert.AreSame(config.Default, stamps.Values[0]);
                Assert.AreEqual(secondBucketTime, stamps.Keys[1]);
                Assert.AreSame(config.Intervals.ElementAt(1), stamps.Values[1]);
                Assert.AreEqual(thirdBucketTime, stamps.Keys[2]);
                Assert.AreSame(config.Intervals.ElementAt(2), stamps.Values[2]);
                Assert.AreEqual(DateTime.MinValue, stamps.Keys[3]);
                Assert.AreSame(config.Intervals.ElementAt(3), stamps.Values[3]);
            }
        }

        [Test]
        public void CanConvertConfigurationToAndFromJSON()
        {
            var serializer = new JsonSerializer();
            string json;
            using (var writer = new StringWriter())
            {
                serializer.Serialize(writer, this.customCompaction);
                json = writer.ToString();
            }

            using (var reader = new StringReader(json))
            using (var jsonReader = new JsonTextReader(reader))
            {
                var deserialized = serializer.Deserialize<DataCompactionConfiguration>(jsonReader);
                Assert.IsTrue(this.customCompaction.Intervals.SetEquals(deserialized.Intervals));
            }
        }

        private static DateTime RoundTimeStamp(DateTime source, long roundToTicks)
        {
            long ticks = (source.Ticks + (roundToTicks / 2) + 1) / roundToTicks;
            return new DateTime(ticks * roundToTicks, DateTimeKind.Utc);
        }
    }
}
