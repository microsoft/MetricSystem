// ---------------------------------------------------------------------
// <copyright file="DataManager.cs" company="Microsoft">
//     Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//     Information Contained Herein is Proprietary and Confidential.
// </copyright>
// --------------------------------------------------------------------

namespace MetricSystem.Data.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    using Bond;

    using MetricSystem.Data;
    using MetricSystem.Utilities;

    using NUnit.Framework;

    [TestFixture]
    public sealed class DataManagerTests
    {
        private DataManager dataManager;
        private const string AnyCounterName = "/anyCounter";

        private static readonly DimensionSet AnyKeys = new DimensionSet(
            new HashSet<Dimension>
            {
                new Dimension("foo"),
                new Dimension("bar"),
                new Dimension("baz"),
            });
        private static readonly DimensionSpecification AnyDimensions = new DimensionSpecification();
        const string AnyDimValue = "dv";

        private readonly DataCompactionConfiguration defaultCompactionConfig =
            new DataCompactionConfiguration(new List<DataIntervalConfiguration>
                                     {
                                         new DataIntervalConfiguration(TimeSpan.FromMinutes(5), TimeSpan.MaxValue)
                                     });
        [SetUp]
        public void SetUp()
        {
            this.dataManager = new DataManager(null, null, 0, 0, 10);
            this.dataManager.CompactionConfiguration = this.defaultCompactionConfig;

            foreach (var d in AnyKeys.Dimensions) AnyDimensions[d.Name] = AnyDimValue;
        }

        [TearDown]
        public void TearDown()
        {
            this.dataManager.Dispose();
        }

        [Test]
        public async Task CanCreateHitCounter()
        {
            await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
        }

        [Test]
        public void CreateHitCounterWithNullOrEmptyNameThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(async () => await this.dataManager.CreateHitCounter(null, AnyKeys));
            Assert.Throws<ArgumentException>(async () => await this.dataManager.CreateHitCounter(string.Empty, AnyKeys));
            Assert.Throws<ArgumentException>(async () => await this.dataManager.CreateHitCounter("  ", AnyKeys));
        }

        [Test]
        public void CreateHitCounterWithNullKeySetThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(async () =>
                                                 await this.dataManager.CreateHitCounter(AnyCounterName, null));
        }

        [Test]
        public async Task CreateHitCounterWithEmptyKeySetIsValid()
        {
            await this.dataManager.CreateHitCounter(AnyCounterName, new DimensionSet(new HashSet<Dimension>()));
        }

        [Test]
        public void CreateHitCounterWithNullOrEmptyDimensionsThrowsArgumentException()
        {
            Assert.Throws<ArgumentException>(async () =>
                                             await
                                             this.dataManager.CreateHitCounter(AnyCounterName,
                                                                               new DimensionSet(new HashSet<Dimension>
                                                                                                {
                                                                                                    null
                                                                                                })));
            Assert.Throws<ArgumentException>(async () =>
                                             await
                                             this.dataManager.CreateHitCounter(AnyCounterName,
                                                                               new DimensionSet(new HashSet<Dimension>
                                                                                                {new Dimension(" ")})));
            Assert.Throws<ArgumentException>(async () =>
                                             await this.dataManager.CreateHitCounter(AnyCounterName,
                                                                                     new DimensionSet(new HashSet
                                                                                                          <Dimension>
                                                                                                      {
                                                                                                          new Dimension(
                                                                                                              string
                                                                                                                  .Empty)
                                                                                                      })));
        }

        [Test]
        public async Task CreateHistogramCounterSupportsShortKeyType()
        {
            await this.dataManager.CreateHistogramCounter(AnyCounterName, AnyKeys);
        }

        [Test]
        public async Task CreateHistogramCounterSupportsIntKeyType()
        {
            await this.dataManager.CreateHistogramCounter(AnyCounterName, AnyKeys);
        }

        [Test]
        public async Task CreateHistogramCounterSupportsLongKeyType()
        {
            await this.dataManager.CreateHistogramCounter(AnyCounterName, AnyKeys);
        }

        [Test]
        public void CreatingDuplicateCountersThrowsInvalidOperationException()
        {
            const string anyCounterName = "/anyCounter";

            Assert.Throws<InvalidOperationException>(async () =>
                                                     {
                                                         await this.dataManager.CreateHitCounter(anyCounterName, AnyKeys);
                                                         await this.dataManager.CreateHitCounter(anyCounterName, AnyKeys);
                                                     });

            Assert.Throws<InvalidOperationException>(async () =>
                                                     {
                                                         await this.dataManager.CreateHitCounter(anyCounterName, AnyKeys);
                                                         await
                                                             this.dataManager.CreateHistogramCounter(
                                                                                                           anyCounterName,
                                                                                                           AnyKeys);
                                                     });
            Assert.Throws<InvalidOperationException>(async () =>
                                                     {
                                                         await this.dataManager.CreateHistogramCounter(anyCounterName,
                                                                                                      AnyKeys);
                                                         await this.dataManager.CreateHistogramCounter(anyCounterName,
                                                                                                      AnyKeys);
                                                     });
        }

        [Test]
        public void DataNamesReturnsEmptyEnumatorForNoData()
        {
            Assert.IsFalse(this.dataManager.Counters.Any());
        }

        [Test]
        public async Task DataNamesReturnsNameOfAllData()
        {
            const string anyCounterName2 = AnyCounterName + "2";

            await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            await this.dataManager.CreateHitCounter(anyCounterName2, AnyKeys);

            var counters = this.dataManager.Counters.ToList();
            Assert.AreEqual(2, counters.Count);
            Assert.IsTrue(counters.Any(c => c.Name == AnyCounterName));
            Assert.IsTrue(counters.Any(c => c.Name == anyCounterName2));
        }

        [Test]
        public async Task AddingDataWithMissingDimensionsThrowsArgumentNullException()
        {
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            Assert.Throws<ArgumentNullException>(() => counter.Increment(null));
        }

        [Test]
        public async Task AddingDataWithEmptyKeyValuesIsValid()
        {
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            var dimensions = new DimensionSpecification();
            foreach (var d in AnyKeys.Dimensions) dimensions[d.Name] = string.Empty;
            counter.Increment(dimensions);
        }

        private DataSample CombinedDataQuery(Counter counter, DimensionSpecification queryParams)
        {
            if (queryParams != null)
            {
                queryParams[ReservedDimensions.AggregateSamplesDimension] = "true";
            }

            counter.DataSet.Flush();
            var queryResults = counter.Query(queryParams).ToList();
            queryParams.Remove(ReservedDimensions.AggregateSamplesDimension);

            return queryResults.Count > 0 ? queryResults[0] : null;
        }

        private IEnumerable<DataSample> BucketedDataQuery(Counter counter, DimensionSpecification queryParams,
                                                          int expectedCount = 1)
        {
            if (queryParams != null)
            {
                queryParams[ReservedDimensions.AggregateSamplesDimension] = "false";
            }

            counter.DataSet.Flush();
            var queryResult = counter.Query(queryParams);
            if (queryResult == null)
            {
                return null;
            }
            var bucketedSamplesList = counter.Query(queryParams).ToList();
            Assert.AreEqual(expectedCount, bucketedSamplesList.Count);
            queryParams.Remove(ReservedDimensions.AggregateSamplesDimension);
            return bucketedSamplesList;
        }

        [Test]
        public async Task CanRetrieveIncrementedHitCount()
        {
            var anyTimestamp = DateTime.Now;
            const ulong anyIncrement = 42;

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);

            for (ulong i = 0; i < anyIncrement; ++i)
            {
                counter.Increment(AnyDimensions, anyTimestamp);
            }

            var sample = this.CombinedDataQuery(counter, AnyDimensions);
            Assert.AreEqual(anyIncrement, sample.HitCount);
        }

        [Test]
        public async Task HitCountsAreCorrectlyDistributedByBucket()
        {
            var bucket1Timestamp = DateTime.Now;
            var bucket2Timestamp =
                new DateTime(bucket1Timestamp.Ticks + this.defaultCompactionConfig.DefaultBucketTicks * 2);
            const ulong bucket1Increment = 867;
            const ulong bucket2Increment = 5309;

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            for (ulong i = 0; i < bucket1Increment; ++i)
            {
                counter.Increment(AnyDimensions, bucket1Timestamp);
            }
            for (ulong i = 0; i < bucket2Increment; ++i)
            {
                counter.Increment(AnyDimensions, bucket2Timestamp);
            }

            var sample = this.CombinedDataQuery(counter, AnyDimensions);
            Assert.AreEqual(DataSampleType.HitCount,sample.SampleType);
            Assert.AreEqual(bucket1Increment + bucket2Increment, sample.HitCount);

            var bucketedSamples = this.BucketedDataQuery(counter, AnyDimensions, 2).ToList();
            var sample1 = bucketedSamples[1];
            Assert.AreEqual(DataSampleType.HitCount, sample1.SampleType);
            Assert.AreEqual(bucket1Increment, sample1.HitCount);
            var sample2 = bucketedSamples[0];
            Assert.AreEqual(bucket2Increment, sample2.HitCount);
            Assert.AreEqual(DataSampleType.HitCount, sample2.SampleType);
        }

        [Test]
        public async Task CanRetrieveHistogramData()
        {
            var anyTimestamp = DateTime.Now;
            var histogram = new Dictionary<int, uint> { { 867, 5309 }, { 25, 624 } };

            var counter = await this.dataManager.CreateHistogramCounter(AnyCounterName, AnyKeys);

            foreach (var kvp in histogram)
            {
                for (var i = 0; i < kvp.Value; ++i)
                {
                    counter.AddValue(kvp.Key, AnyDimensions, anyTimestamp);
                }
            }

            var sample = this.CombinedDataQuery(counter, AnyDimensions);
            Assert.AreEqual(DataSampleType.Histogram, sample.SampleType);
            foreach (var kvp in histogram)
            {
                Assert.AreEqual(kvp.Value, sample.Histogram[kvp.Key]);
            }
        }

        [Test]
        public async Task CanRetrieveHistogramPercentileData()
        {
            var anyTimestamp = DateTime.Now;
            var counter = await this.dataManager.CreateHistogramCounter(AnyCounterName, AnyKeys);
            for (int i = 0; i < 100; ++i)
            {
                counter.AddValue(i, AnyDimensions, anyTimestamp);
            }

            var dims = new DimensionSpecification(AnyDimensions);
            for (int i = 1; i < 100; ++i)
            {
                dims[ReservedDimensions.PercentileDimension] = i.ToString();
                var sample =
                    this.CombinedDataQuery(counter, dims);
                Assert.AreEqual(sample.Percentile, i);
                Assert.AreEqual(sample.SampleCount, (ulong)100);
                Assert.AreEqual(sample.PercentileValue, i - 1);
            }

            dims[ReservedDimensions.PercentileDimension] = "0";
            var sample2 = this.CombinedDataQuery(counter, dims);
            Assert.AreEqual(sample2.Percentile, 0);
            Assert.AreEqual(sample2.SampleCount, (ulong)100);
            Assert.AreEqual(sample2.PercentileValue, 0);

            dims[ReservedDimensions.PercentileDimension] = "100";
            sample2 = this.CombinedDataQuery(counter, dims);
            Assert.AreEqual(sample2.Percentile, 100);
            Assert.AreEqual(sample2.SampleCount, (ulong)100);
            Assert.AreEqual(sample2.PercentileValue, 99);
        }

        [Test]
        public async Task CanRetrieveHistogramAverageData()
        {
            var anyTimestamp = DateTime.Now;
            var counter = await this.dataManager.CreateHistogramCounter(AnyCounterName, AnyKeys);
            for (int i = 0; i < 100; ++i)
            {
                counter.AddValue(i, AnyDimensions, anyTimestamp);
            }

            var dims = new DimensionSpecification(AnyDimensions);
            dims[ReservedDimensions.PercentileDimension] = ReservedDimensions.PercentileDimensionValueForAverage;

            var sample = this.CombinedDataQuery(counter, dims);
            Assert.AreEqual(sample.Average, 49.5);
            Assert.AreEqual(sample.SampleCount, (ulong)100);
        }

        [Test]
        public async Task CanRetrieveHistogramMaximumData()
        {
            var anyTimestamp = DateTime.Now;
            var counter = await this.dataManager.CreateHistogramCounter(AnyCounterName, AnyKeys);
            for (int i = 0; i < 100; ++i)
            {
                counter.AddValue(i, AnyDimensions, anyTimestamp);
            }

            var dims = new DimensionSpecification(AnyDimensions);
            dims[ReservedDimensions.PercentileDimension] = ReservedDimensions.PercentileDimensionValueForMaximum;

            var sample = this.CombinedDataQuery(counter, dims);
            Assert.AreEqual(sample.MaxValue, 99);
        }

        [Test]
        public async Task CanRetrieveHistogramMinimumData()
        {
            var anyTimestamp = DateTime.Now;
            var counter = await this.dataManager.CreateHistogramCounter(AnyCounterName, AnyKeys);
            for (int i = 0; i < 100; ++i)
            {
                counter.AddValue(i, AnyDimensions, anyTimestamp);
            }

            var dims = new DimensionSpecification(AnyDimensions);
            dims[ReservedDimensions.PercentileDimension] = ReservedDimensions.PercentileDimensionValueForMinimum;

            var sample = this.CombinedDataQuery(counter, dims);
            Assert.AreEqual(sample.MinValue, 0);
        }

        [Test]
        public async Task NonNumericHistogramValueExceptAverageThrowsFormatException()
        {
            var counter = await this.dataManager.CreateHistogramCounter(AnyCounterName, AnyKeys);
            var dims = new DimensionSpecification(AnyDimensions);
            dims[ReservedDimensions.PercentileDimension] = string.Empty;
            Assert.Throws<FormatException>(() => this.CombinedDataQuery(counter, dims));
            Assert.Throws<FormatException>(() => this.BucketedDataQuery(counter, dims));

            dims[ReservedDimensions.PercentileDimension] = "any random stuff";
            Assert.Throws<FormatException>(() => this.CombinedDataQuery(counter, dims));
            Assert.Throws<FormatException>(() => this.BucketedDataQuery(counter, dims));

            dims[ReservedDimensions.PercentileDimension] = ReservedDimensions.PercentileDimensionValueForAverage;
            var x = this.BucketedDataQuery(counter, dims, 0);
            Assert.IsNotNull(x);
        }

        [Test]
        public async Task InvalidNumericHistogramValueThrowsArgumentOutOfRangeException()
        {
            var counter = await this.dataManager.CreateHistogramCounter(AnyCounterName, AnyKeys);
            var dims = new DimensionSpecification(AnyDimensions);
            dims[ReservedDimensions.PercentileDimension] = "-1";
            Assert.Throws<ArgumentOutOfRangeException>(() => this.CombinedDataQuery(counter, dims));
            Assert.Throws<ArgumentOutOfRangeException>(() => this.BucketedDataQuery(counter, dims));

            dims[ReservedDimensions.PercentileDimension] = "100.1";
            Assert.Throws<ArgumentOutOfRangeException>(() => this.CombinedDataQuery(counter, dims));
            Assert.Throws<ArgumentOutOfRangeException>(() => this.BucketedDataQuery(counter, dims));
        }

        [Test]
        public async Task HistogramCountsAreCorrectlyDistributedByBucket()
        {
            var bucket1Timestamp = DateTime.Now;
            var bucket2Timestamp =
                new DateTime(bucket1Timestamp.Ticks + this.defaultCompactionConfig.DefaultBucketTicks * 2);
            var bucket1Histogram = new Dictionary<int, uint> { { 867, 5309 }, { 25, 624 } };
            var bucket2Histogram = new Dictionary<int, uint> { { 867, 624 }, { 25, 5309 } };

            var counter = await this.dataManager.CreateHistogramCounter(AnyCounterName, AnyKeys);
            foreach (var kvp in bucket1Histogram)
            {
                for (var i = 0; i < kvp.Value; ++i)
                {
                    counter.AddValue(kvp.Key, AnyDimensions, bucket1Timestamp);
                }
            }
            foreach (var kvp in bucket2Histogram)
            {
                for (var i = 0; i < kvp.Value; ++i)
                {
                    counter.AddValue(kvp.Key, AnyDimensions, bucket2Timestamp);
                }
            }

            var sample = this.CombinedDataQuery(counter, AnyDimensions);
            Assert.AreEqual(DataSampleType.Histogram, sample.SampleType);
            foreach (var kvp in bucket1Histogram)
            {
                Assert.AreEqual(kvp.Value + bucket2Histogram[kvp.Key], sample.Histogram[kvp.Key]);
            }

            var bucketedSamples = this.BucketedDataQuery(counter, AnyDimensions, 2).ToList();
            var sample1 = bucketedSamples[1];
            Assert.AreEqual(DataSampleType.Histogram, sample1.SampleType);
            foreach (var kvp in bucket1Histogram)
            {
                Assert.AreEqual(kvp.Value, sample1.Histogram[kvp.Key]);
            }
            var sample2 = bucketedSamples[0];
            Assert.AreEqual(DataSampleType.Histogram, sample2.SampleType);
            foreach (var kvp in bucket2Histogram)
            {
                Assert.AreEqual(kvp.Value, sample2.Histogram[kvp.Key]);
            }
        }

        [Test]
        public async Task HistogramPercentileValuesAreCorrectlyDistributedByBucket()
        {
            var bucket1Timestamp = DateTime.Now;
            var bucket2Timestamp =
                new DateTime(bucket1Timestamp.Ticks + this.defaultCompactionConfig.DefaultBucketTicks * 2);

            var counter = await this.dataManager.CreateHistogramCounter(AnyCounterName, AnyKeys);
            for (int i = 0; i < 100; ++i)
            {
                counter.AddValue(i, AnyDimensions, bucket1Timestamp);
            }
            for (int i = 0; i < 100; ++i)
            {
                counter.AddValue(i * 10, AnyDimensions, bucket2Timestamp);
            }

            var dims = new DimensionSpecification(AnyDimensions);
            for (int i = 1; i < 100; ++i)
            {
                dims[ReservedDimensions.PercentileDimension] = i.ToString();
                var bucketedSamples =
                    this.BucketedDataQuery(counter, dims, 2).ToList();
                var sample1 = bucketedSamples[1];
                Assert.AreEqual(sample1.Percentile, i);
                Assert.AreEqual(sample1.SampleCount, (ulong)100);
                Assert.AreEqual(sample1.PercentileValue, i - 1);

                var sample2 = bucketedSamples[0];
                Assert.AreEqual(sample2.Percentile, i);
                Assert.AreEqual(sample2.SampleCount, (ulong)100);
                Assert.AreEqual(sample2.PercentileValue, (i - 1) * 10);
            }
        }

        [Test]
        public async Task GetCombinedDataPopulatesCounterNameDimensionsAndTimes()
        {
            var anyTimestamp = DateTime.Now;

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            counter.Increment(AnyDimensions, anyTimestamp);

            var sample = this.CombinedDataQuery(counter, AnyDimensions);
            Assert.AreEqual(DataSampleType.HitCount, sample.SampleType);
            Assert.AreEqual(AnyCounterName, sample.Name);
            foreach (var kvp in AnyDimensions)
            {
                Assert.AreEqual(sample.Dimensions[kvp.Key], kvp.Value);
            }
            Assert.AreEqual(
                new DataBucket<InternalHitCount>(AnyKeys, anyTimestamp,
                    this.defaultCompactionConfig.DefaultBucketTicks, null, null).StartTime, 
                    sample.StartTime.ToDateTime());
            Assert.AreEqual(
                new DataBucket<InternalHitCount>(AnyKeys, anyTimestamp,
                    this.defaultCompactionConfig.DefaultBucketTicks, null, null).EndTime,
                            sample.EndTime.ToDateTime());
        }

        [Test]
        public async Task GetBucketedDataThrowsArgumentNullExceptionForNullFilterDimensions()
        {
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            Assert.Throws<ArgumentNullException>(() => this.BucketedDataQuery(counter, null));
        }

        [Test]
        public async Task GetBucketedDataPopulatesCounterNameDimensionsAndTimes()
        {
            var bucket1Timestamp = DateTime.Now;
            var bucket2Timestamp =
                new DateTime(bucket1Timestamp.Ticks + this.defaultCompactionConfig.DefaultBucketTicks * 2);

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            counter.Increment(AnyDimensions, bucket1Timestamp);
            counter.Increment(AnyDimensions, bucket2Timestamp);

            var bucketedSamples = this.BucketedDataQuery(counter, AnyDimensions, 2).ToList();

            // These come back via a stack, so the second item is the first time bucket. Ugh, implementation details.
            var sample1 = bucketedSamples[1];
            Assert.AreEqual(
                new DataBucket<InternalHitCount>(AnyKeys, bucket1Timestamp,
                    this.defaultCompactionConfig.DefaultBucketTicks, null, null).StartTime,
                 sample1.StartTime.ToDateTime());
            Assert.AreEqual(
                new DataBucket<InternalHitCount>(AnyKeys, bucket1Timestamp,
                    this.defaultCompactionConfig.DefaultBucketTicks, null, null).EndTime,
                 sample1.EndTime.ToDateTime());

            var sample2 = bucketedSamples[0];
            Assert.AreEqual(
                new DataBucket<InternalHitCount>(AnyKeys, bucket2Timestamp,
                    this.defaultCompactionConfig.DefaultBucketTicks, null, null).StartTime,
                 sample2.StartTime.ToDateTime());
            Assert.AreEqual(
                new DataBucket<InternalHitCount>(AnyKeys, bucket2Timestamp,
                    this.defaultCompactionConfig.DefaultBucketTicks, null, null).EndTime,
                 sample2.EndTime.ToDateTime());

            foreach (var ds in bucketedSamples)
            {
                var sample = ds;
                Assert.AreEqual(DataSampleType.HitCount, sample.SampleType);
                Assert.AreEqual(AnyCounterName, sample.Name);
                foreach (var kvp in AnyDimensions)
                {
                    Assert.AreEqual(sample.Dimensions[kvp.Key], kvp.Value);
                }
            }
        }

        [Test]
        public async Task GetCombinedDataThrowsArgumentNullExceptionForNullFilterDimensions()
        {
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            Assert.Throws<ArgumentNullException>(() => this.CombinedDataQuery(counter, null));
        }

        [Test]
        public async Task GetCombinedDataReturnsNullIfNoSamplesMatchDimensionValues()
        {
            var anyTimestamp = DateTime.Now;

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            counter.Increment(AnyDimensions, anyTimestamp);

            var dimValues = new DimensionSpecification();
            foreach (var d in AnyKeys.Dimensions) dimValues[d.Name] = "no match";
            Assert.IsNull(this.CombinedDataQuery(counter, dimValues));
        }

        [Test]
        public async Task GetCombinedDataCombinesDataAcrossDimensionValuesIfFilterValueIsNotProvided()
        {
            var anyTimestamp = DateTime.Now;
            const string firstDimSecondValue = AnyDimValue + "2";
            var dimValues = new DimensionSpecification(AnyDimensions);

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            counter.Increment(dimValues, anyTimestamp);

            dimValues[AnyKeys.Dimensions.First().Name] = firstDimSecondValue;
            counter.Increment(dimValues, anyTimestamp);

            dimValues.Remove(AnyKeys.Dimensions.First().Name);
            var sample = this.CombinedDataQuery(counter, dimValues);
            Assert.AreEqual(DataSampleType.HitCount, sample.SampleType);
            Assert.AreEqual((ulong)2, sample.HitCount);
        }

        [Test]
        public async Task GetCombinedDataCombinesFiltersDataToOnlyMatchingDimensions()
        {
            var anyTimestamp = DateTime.Now;
            const string firstDimSecondValue = AnyDimValue + "2";
            var dimValues = new DimensionSpecification(AnyDimensions);

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            counter.Increment(dimValues, anyTimestamp);

            dimValues[AnyKeys.Dimensions.First().Name] = firstDimSecondValue;
            counter.Increment(dimValues, anyTimestamp);

            dimValues[AnyKeys.Dimensions.First().Name] = AnyDimValue;
            var sample = this.CombinedDataQuery(counter, dimValues);
            Assert.AreEqual(DataSampleType.HitCount, sample.SampleType);
            Assert.AreEqual((ulong)1, sample.HitCount);
        }

        [Test]
        public async Task GetBucketedDataReturnsEmptyEnumerationIfNoSamplesMatchDimensionValues()
        {
            var anyTimestamp = DateTime.Now;

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            counter.Increment(AnyDimensions, anyTimestamp);

            var dimValues = new DimensionSpecification();
            foreach (var d in AnyKeys.Dimensions) dimValues[d.Name] = "no match";
            Assert.IsFalse(this.BucketedDataQuery(counter, dimValues, 0).Any());
        }

        [Test]
        public async Task GetBucketedDataCombinesDataAcrossDimensionValuesIfFilterValueIsNotProvided()
        {
            var anyTimestamp = DateTime.Now;
            const string FirstDimension = "1st";
            const string SecondDimension = "2nd";
            const string ThirdDimension = "3rd";

            var dimensions = new DimensionSet(new HashSet<Dimension>
                                              {
                                                  new Dimension(FirstDimension),
                                                  new Dimension(SecondDimension),
                                                  new Dimension(ThirdDimension)
                                              });
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, dimensions);


            var dimValues = new DimensionSpecification();
            for (int i = 0; i < 10; ++i)
            {
                dimValues[FirstDimension] = i.ToString();
                for (int j = 0; j < 10; ++j)
                {
                    dimValues[SecondDimension] = j.ToString();
                    for (int k = 0; k < 10; ++k)
                    {
                        dimValues[ThirdDimension] = k.ToString();
                        counter.Increment(dimValues, anyTimestamp);
                    }
                }
            }

            var sample =
                this.BucketedDataQuery(counter, new DimensionSpecification()).First();
            Assert.AreEqual(DataSampleType.HitCount, sample.SampleType);
            Assert.AreEqual((ulong)1000, sample.HitCount);

            dimValues.Clear();
            dimValues[FirstDimension] = "8"; // take one tenth of things by filter.
            sample = this.BucketedDataQuery(counter, dimValues).First();
            Assert.AreEqual((ulong)100, sample.HitCount);

            dimValues[SecondDimension] = "6";
            sample = this.BucketedDataQuery(counter, dimValues).First();
            Assert.AreEqual((ulong)10, sample.HitCount);

            dimValues[ThirdDimension] = "7";
            sample = this.BucketedDataQuery(counter, dimValues).First();
            Assert.AreEqual((ulong)1, sample.HitCount);
        }

        [Test]
        public async Task GetBucketedDataSplitsDataByDimension()
        {
            var anyTimestamp = DateTime.Now;
            const string FirstDimension = "1st";
            const string SecondDimension = "2nd";
            const string ThirdDimension = "3rd";

            var dimensions = new DimensionSet(new HashSet<Dimension>
                                              {
                                                  new Dimension(FirstDimension),
                                                  new Dimension(SecondDimension),
                                                  new Dimension(ThirdDimension)
                                              });
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, dimensions);


            var dimValues = new DimensionSpecification();
            for (int i = 0; i < 10; ++i)
            {
                dimValues[FirstDimension] = i.ToString();
                for (int j = 0; j < 10; ++j)
                {
                    dimValues[SecondDimension] = j.ToString();
                    for (int k = 0; k < 10; ++k)
                    {
                        dimValues[ThirdDimension] = k.ToString();
                        counter.Increment(dimValues, anyTimestamp);
                    }
                }
            }

            // Slice without any other filtering.
            dimValues.Clear();
            dimValues[ReservedDimensions.DimensionDimension] = FirstDimension;
            var samples = this.BucketedDataQuery(counter, dimValues, 10);
            var returnedDimensionValues = new HashSet<string>();
            foreach (var baseSample in samples)
            {
                var sample = baseSample;
                Assert.AreEqual((ulong)100, sample.HitCount);
                Assert.AreEqual(1, sample.Dimensions.Count);
                Assert.IsTrue(sample.Dimensions.ContainsKey(FirstDimension));
                Assert.IsFalse(returnedDimensionValues.Contains(sample.Dimensions[FirstDimension]));
                returnedDimensionValues.Add(sample.Dimensions[FirstDimension]);
            }
            Assert.AreEqual(10, returnedDimensionValues.Count);

            // Now slice by one dimension and split by another.
            dimValues[FirstDimension] = "4";
            dimValues[ReservedDimensions.DimensionDimension] = SecondDimension;
            samples = this.BucketedDataQuery(counter, dimValues, 10);
            returnedDimensionValues.Clear();
            foreach (var baseSample in samples)
            {
                var sample = baseSample;
                Assert.AreEqual((ulong)10, sample.HitCount);
                Assert.AreEqual(2, sample.Dimensions.Count);
                Assert.AreEqual("4", sample.Dimensions[FirstDimension]);
                Assert.IsTrue(sample.Dimensions.ContainsKey(SecondDimension));
                Assert.IsFalse(returnedDimensionValues.Contains(sample.Dimensions[SecondDimension]));
                returnedDimensionValues.Add(sample.Dimensions[SecondDimension]);
            }
            Assert.AreEqual(10, returnedDimensionValues.Count);

        }

        [Test]
        public async Task GetBucketedDataCombinesFiltersDataToOnlyMatchingDimensions()
        {
            var anyTimestamp = DateTime.Now;
            const string firstDimSecondValue = AnyDimValue + "2";
            var dimValues = new DimensionSpecification(AnyDimensions);

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            counter.Increment(dimValues, anyTimestamp);

            dimValues[AnyKeys.Dimensions.First().Name] = firstDimSecondValue;
            counter.Increment(dimValues, anyTimestamp);

            dimValues[AnyKeys.Dimensions.First().Name] = AnyDimValue;
            var sample = this.BucketedDataQuery(counter, dimValues).First();
            Assert.AreEqual(DataSampleType.HitCount, sample.SampleType);
            Assert.AreEqual((ulong)1, sample.HitCount);
        }

        [Test]
        public async Task GetDimensionsForDataWithNoDimensionsReturnsEmptyEnumeration()
        {
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, new DimensionSet(new HashSet<Dimension>()));
            Assert.IsFalse(counter.Dimensions.Any());
        }

        [Test]
        public async Task GetDimensionsForDataReturnsAllDimensionsOnce()
        {
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            var dims = counter.Dimensions.ToList();

            Assert.AreEqual(AnyKeys.Dimensions.Count(), dims.Count);
            foreach (var k in AnyKeys.Dimensions)
            {
                Assert.IsTrue(dims.Contains(k.Name));
            }
        }

        [Test]
        public async Task GetDimensionValuesForDataWithNoDimensionsThrowsKeyNotFoundException()
        {
            var counter =
                await this.dataManager.CreateHitCounter(AnyCounterName, new DimensionSet(new HashSet<Dimension>()));
            Assert.Throws<KeyNotFoundException>(
                                                () =>
                                                counter.GetDimensionValues(AnyKeys.Dimensions.First().Name,
                                                                           new DimensionSpecification()));
        }

        [Test]
        public async Task GetDimensionValuesForUnknownDimensionInDataThrowsKeyNotFoundException()
        {
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            Assert.Throws<KeyNotFoundException>(
                                                () =>
                                                counter.GetDimensionValues(string.Empty, new DimensionSpecification()));
        }

        [Test]
        public async Task GetDimensionValuesForUnknownFilterDimensionDoesNotThrow()
        {
            var filterDims = new DimensionSpecification {{"UnknownDim", "UnKnown"}};
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            // if the following call throws test will fail
            counter.GetDimensionValues(AnyKeys.Dimensions.First().Name, filterDims);
        }

        [Test]
        public async Task GetDimensionValuesThrowsArgumentNullExceptionForNullFilterDimensions()
        {
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            Assert.Throws<ArgumentNullException>(() => counter.GetDimensionValues(AnyKeys.Dimensions.First().Name, null));
        }
        
        [Test]
        public async Task GetDimensionValuesForAllDimensionInDataProvidesExpectedValues()
        {
            const int NumValuesPerDim = 100;
            var dimensionValues = new DimensionSpecification();

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);

            for (var i = 0; i < NumValuesPerDim; ++i)
            {
                foreach (var d in AnyKeys.Dimensions)
                {
                    dimensionValues[d.Name] = d.Name + i;
                }
                counter.Increment(dimensionValues);
            }
            counter.DataSet.Flush();

            foreach (var d in AnyKeys.Dimensions)
            {
                var values = counter.GetDimensionValues(d.Name, new DimensionSpecification()).ToList();
                Assert.AreEqual(NumValuesPerDim, values.Count);
                for (var i = 0; i < NumValuesPerDim; ++i)
                {
                    var expected = d.Name + i;
                    Assert.IsTrue(values.Contains(expected));
                }
            }
        }

        [Test]
        public async Task TimeFilterThrowsFormatExceptionIfTimeFilterIsInvalid()
        {
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            var filter = new DimensionSpecification
                         {
                             {ReservedDimensions.StartTimeDimension, "not a valid time"},
                             {ReservedDimensions.EndTimeDimension, DateTime.Now.ToString()},
                         };
            Assert.Throws<FormatException>(() => this.BucketedDataQuery(counter, filter));

            filter = new DimensionSpecification
                     {
                         {ReservedDimensions.StartTimeDimension, DateTime.Now.ToString()},
                         {ReservedDimensions.EndTimeDimension, string.Empty}
                     };
            Assert.Throws<FormatException>(() => this.BucketedDataQuery(counter, filter));
        }

        [Test]
        public async Task TimeFilterThrowsArgumentOutOfRangeExceptionIfStartTimeIsSameOrGreaterThanEndTime()
        {
            var anyTime = DateTime.Now;

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            var filter = new DimensionSpecification
                         {
                             {ReservedDimensions.StartTimeDimension, anyTime.ToString()},
                             {ReservedDimensions.EndTimeDimension, anyTime.ToString()},
                         };

            Assert.Throws<ArgumentOutOfRangeException>(() => this.BucketedDataQuery(counter, filter));

            var anyLaterTime = anyTime.Add(new TimeSpan(0, 0, 1));
            filter[ReservedDimensions.StartTimeDimension] = anyLaterTime.ToString();
            Assert.Throws<ArgumentOutOfRangeException>(() => this.BucketedDataQuery(counter, filter));
        }

        [Test]
        public async Task GetDimensionValuesWithTimeFilteredProvidesValuesOnlyInTimeRange()
        {
            var earlyTime = new DateTime(2013, 12, 11, 00, 01, 00);//bucket starttime  = 2013, 12, 11,00,00,00, endtime = 2013, 12, 11,00,20,00
            var laterTime = new DateTime(2013, 12, 11, 00, 22, 00);//bucket starttime  = 2013, 12, 11,00,20,00, endtime = 2013, 12, 11,00,40,00

            var dimensionValues = new DimensionSpecification(AnyDimensions);

            //create the hit counters
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            dimensionValues[AnyKeys.Dimensions.First().Name] = "early";
            counter.Increment(dimensionValues, earlyTime);
            dimensionValues[AnyKeys.Dimensions.First().Name] = "later";
            counter.Increment(dimensionValues, laterTime);
            counter.DataSet.Flush();

            //this should return early
            var filter = new DimensionSpecification
                         {
                             {ReservedDimensions.StartTimeDimension, new DateTime(2013, 12, 11,00,00,00).ToString(Protocol.TimestampStringFormat)},
                             {ReservedDimensions.EndTimeDimension, new DateTime(2013,12,11,00,20,00).ToString(Protocol.TimestampStringFormat)},
                         };
            var values = counter.GetDimensionValues(AnyKeys.Dimensions.First().Name, filter).ToList();
            Assert.AreEqual(1, values.Count);
            Assert.AreEqual("early", values[0]);

            //should only return later
            filter = new DimensionSpecification
                         {
                             {ReservedDimensions.StartTimeDimension, new DateTime(2013, 12, 11,00,20,00).ToString(Protocol.TimestampStringFormat)},
                             {ReservedDimensions.EndTimeDimension, new DateTime(2013,12,11,00,40,00).ToString(Protocol.TimestampStringFormat)},
                         };
            values = counter.GetDimensionValues(AnyKeys.Dimensions.First().Name, filter).ToList();
            Assert.AreEqual(1, values.Count);
            Assert.AreEqual("later", values[0]);

            //should return both later and early
            filter = new DimensionSpecification
                         {
                             {ReservedDimensions.StartTimeDimension, new DateTime(2013, 12, 11,00,00,00).ToString(Protocol.TimestampStringFormat)},
                             {ReservedDimensions.EndTimeDimension, new DateTime(2013,12,11,00,40,00).ToString(Protocol.TimestampStringFormat)},
                         };
            values = counter.GetDimensionValues(AnyKeys.Dimensions.First().Name, filter).ToList();
            Assert.AreEqual(2, values.Count);
            Assert.IsTrue(values.Contains("early"));
            Assert.IsTrue(values.Contains("later"));
        }

        [Test]
        public async Task GetDimensionValuesForAllDimensionInDataProvidesEmptyStringForEmptyDimensionValue()
        {
            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            counter.Increment(AnyDimensions);
            var otherDimValues = new DimensionSpecification(AnyDimensions);
            otherDimValues[AnyKeys.Dimensions.First().Name] = ReservedDimensions.EmptyDimensionValue;
            counter.Increment(otherDimValues);
            counter.DataSet.Flush();

            var values =
                counter.GetDimensionValues(AnyKeys.Dimensions.First().Name, new DimensionSpecification()).ToList();
            Assert.AreEqual(2, values.Count);
            Assert.IsTrue(values.Contains(AnyDimValue));
            Assert.IsTrue(values.Contains(string.Empty));
        }

        [Test]
        public async Task GetDimensionValuesWithAllDimensionsFilledThrowsArgumentException()
        {
            var dimValues = new DimensionSpecification(AnyDimensions);

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            Assert.Throws<ArgumentException>(
                                             () =>
                                             counter.GetDimensionValues(AnyKeys.Dimensions.First().Name, dimValues)
                                                    .ToList());
        }

        [Test]
        public async Task GetDimensionValuesForAllFilteredDimensionInDataProvidesExpectedValues()
        {
            var anyTimestamp = DateTime.Now;
            const string firstDimSecondValue = AnyDimValue + "2";
            var dimValues = new DimensionSpecification(AnyDimensions);

            var counter = await this.dataManager.CreateHitCounter(AnyCounterName, AnyKeys);
            counter.Increment(dimValues, anyTimestamp);

            dimValues[AnyKeys.Dimensions.First().Name] = firstDimSecondValue;
            counter.Increment(dimValues, anyTimestamp);

            dimValues.Remove(AnyKeys.Dimensions.First().Name);
            counter.DataSet.Flush();
            var values = counter.GetDimensionValues(AnyKeys.Dimensions.First().Name, dimValues).ToList();
            Assert.AreEqual(2, values.Count);
            Assert.IsTrue(values.Contains(AnyDimValue));
            Assert.IsTrue(values.Contains(firstDimSecondValue));

            dimValues[AnyKeys.Dimensions.First().Name] = firstDimSecondValue;
            counter.Increment(dimValues, anyTimestamp);

            const string lastDimSecondValue = AnyDimValue + "2";
            dimValues[AnyKeys.Dimensions.Last().Name] = lastDimSecondValue;
            counter.Increment(dimValues, anyTimestamp);

            var secondKey = AnyKeys.Dimensions.Skip(1).First();
            dimValues.Remove(secondKey.Name);
            counter.DataSet.Flush();
            values = counter.GetDimensionValues(AnyKeys.Dimensions.First().Name, dimValues).ToList();
            Assert.AreEqual(1, values.Count);
            Assert.IsTrue(values.Contains(firstDimSecondValue));

            values = counter.GetDimensionValues(secondKey.Name, dimValues).ToList();
            Assert.AreEqual(1, values.Count);
            Assert.IsTrue(values.Contains(AnyDimValue));
        }
    }
}
