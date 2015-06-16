namespace MetricSystem.Client.UnitTests
{
    using System;
    using System.Collections.Generic;

    using MetricSystem;

    using MetricSystem.Utilities;

    using NUnit.Framework;

    [TestFixture]
    public class SampleCombinerTests
    {
        private readonly DateTime start = DateTime.Now;
        private readonly DateTime end = DateTime.Now.AddSeconds(10);

        [Test]
        public void SampleCombinerFactoryChecksArguments()
        {
            Assert.Throws<ArgumentNullException>(() => SampleCombiner.Create(null));

            var garbageDataSample = new DataSample();
            garbageDataSample.SampleType = DataSampleType.None;

            Assert.Throws<ArgumentException>(() => new SampleCombiner(garbageDataSample));

            garbageDataSample.SampleType = (DataSampleType)100;
            Assert.Throws<ArgumentException>(() => new SampleCombiner(garbageDataSample));

            var percentileSample = new DataSample();
            Assert.Throws<ArgumentException>(() => new SampleCombiner(percentileSample));
        }

        [Test]
        public void SampleCombinerCopiesInitialData()
        {
            var hitCountSample = new DataSample { HitCount = 400, SampleType = DataSampleType.HitCount, StartTime = this.start.ToMillisecondTimestamp(), EndTime = this.end.ToMillisecondTimestamp() };

            var combiner = new SampleCombiner(hitCountSample);
            Assert.IsNotNull(combiner.Data);
            Assert.AreEqual(DataSampleType.HitCount, combiner.Data.SampleType);
            Assert.AreEqual(400, (int)(combiner.Data).HitCount);
        }

        [Test]
        public void TestHitCountSampleCombiner()
        {
            var hitCountSample = new DataSample { HitCount = 1, SampleType = DataSampleType.HitCount, StartTime = this.start.ToMillisecondTimestamp(), EndTime = this.end.ToMillisecondTimestamp() };
            var combiner = new SampleCombiner(hitCountSample);

            for (uint i = 0; i < 100; i++)
            {
                var before = (combiner.Data).HitCount;
                combiner.AddSample(new DataSample { HitCount = i, SampleType = DataSampleType.HitCount, StartTime = this.start.ToMillisecondTimestamp(), EndTime = this.end.ToMillisecondTimestamp() });
                Assert.AreEqual(before + i, (combiner.Data).HitCount);
            }
        }

        [Test]
        public void TestHistogramSampleCombiner()
        {
            var initialSample = new DataSample
                                {
                                    SampleType = DataSampleType.Histogram,
                                    Histogram = new Dictionary<long, uint> {{1, 1}, {2, 1}},
                                    StartTime = this.start.ToMillisecondTimestamp(),
                                    EndTime = this.end.ToMillisecondTimestamp()
                                };
            var combiner = new SampleCombiner(initialSample);

            var doubleSample = new DataSample
                               {
                                   Histogram = new Dictionary<long, uint> {{1, 2}, {10, 2}}
                               };
            for (int i = 0; i < 100; i++)
            {
                combiner.AddData(doubleSample);
            }

            var result = combiner.Data;
            Assert.AreEqual((uint)201, result.Histogram[1]);
            Assert.AreEqual((uint)1, result.Histogram[2]);
            Assert.AreEqual((uint)200, result.Histogram[10]);
        }

        [Test]
        public void TestMaximumSampleCombiner()
        {
            var initialSample = new DataSample
                                {
                                    SampleType = DataSampleType.Maximum,
                                    MaxValue = 100,
                                    SampleCount = 100,
                                    StartTime = this.start.ToMillisecondTimestamp(),
                                    EndTime = this.end.ToMillisecondTimestamp()
                                };

            var combiner = new SampleCombiner(initialSample);
            var addMe = new DataSample { SampleType = DataSampleType.Maximum, SampleCount = 1000 };
            for (int i = 0; i < 200; i++)
            {
                addMe.MaxValue = i;
                combiner.AddData(addMe);
            }

            Assert.AreEqual(199, combiner.Data.MaxValue);
            Assert.AreEqual((ulong)200100, combiner.Data.SampleCount);
        }

        [Test]
        public void TestAverageSampleCombiner()
        {
            var initialSample = new DataSample
                                {
                                    SampleType = DataSampleType.Average,
                                    Average = 1.0,
                                    SampleCount = 100,
                                    StartTime = this.start.ToMillisecondTimestamp(),
                                    EndTime = this.end.ToMillisecondTimestamp()
                                };

            var combiner = new SampleCombiner(initialSample);
            var addMe = new DataSample { Average = 1.0, SampleCount = 1000, SampleType = DataSampleType.Average};
            for (int i = 0; i < 100; i++)
            {
                combiner.AddData(addMe);
            }

            Assert.AreEqual(1.0, combiner.Data.Average);
            Assert.IsTrue(combiner.Data.SampleCount > 100);
        }

        [Test]
        public void TestAverageSampleCombinerDoesNotOverflow()
        {
            const int iterations = 10;
            var theSample = new DataSample
            {
                SampleType = DataSampleType.Average,
                Average = double.MaxValue,
                SampleCount = ulong.MaxValue / (iterations + 1),
                StartTime = this.start.ToMillisecondTimestamp(),
                EndTime = this.end.ToMillisecondTimestamp()
            };

            var combiner = new SampleCombiner(theSample);
            for (int i = 0; i < iterations; i++)
            {
                combiner.AddData(theSample);
            }
        }

        [Test]
        public void TestMinimumSampleCombiner()
        {
            var initialSample = new DataSample
            {
                SampleType = DataSampleType.Minimum,
                MinValue = 100,
                SampleCount = 100,
                StartTime = this.start.ToMillisecondTimestamp(),
                EndTime = this.end.ToMillisecondTimestamp()
            };

            var combiner = new SampleCombiner(initialSample);
            var addMe = new DataSample { SampleCount = 1000, SampleType = DataSampleType.Minimum};
            for (int i = 0; i < 200; i++)
            {
                addMe.MinValue = i;
                combiner.AddData(addMe);
            }

            Assert.AreEqual(0, combiner.Data.MinValue);
            Assert.AreEqual((ulong)200100, combiner.Data.SampleCount);
        }

        [Test]
        public void SampleCombinerMergesTimeRanges()
        {
            var now = DateTime.Now;
            var today = new DateTime(now.Year, now.Month, now.Day);
            var yesterday = today.AddDays(-1);
            var tomorrow = today.AddDays(1);

            var firstSample = new DataSample { HitCount = 1, SampleType = DataSampleType.HitCount, StartTime = yesterday.ToMillisecondTimestamp(), EndTime = today.ToMillisecondTimestamp() };
            var secondSample = new DataSample { HitCount = 1, SampleType = DataSampleType.HitCount, StartTime = today.ToMillisecondTimestamp(), EndTime = tomorrow.ToMillisecondTimestamp() };
            var combiner = new SampleCombiner(firstSample);
            combiner.AddSample(secondSample);

            Assert.AreEqual(yesterday.ToMillisecondTimestamp(), combiner.Data.StartTime);
            Assert.AreEqual(tomorrow.ToMillisecondTimestamp(), combiner.Data.EndTime);
        }
    }
}
