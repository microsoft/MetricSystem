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

    using Microsoft.IO;
    using MetricSystem.Utilities;

    using NUnit.Framework;

    sealed class MockSharedDataSetProperties : ISharedDataSetProperties
    {
        private static readonly RecyclableMemoryStreamManager memoryStreamManager
            = new RecyclableMemoryStreamManager(1 << 17, 1 << 20, 1 << 24);

        public MockSharedDataSetProperties()
        {
            this.StoragePath = null;
            this.LocalSourceName = "TESTSOURCE";
            this.SealTime = TimeSpan.Zero;
            this.MaximumDataAge = TimeSpan.Zero;
            this.CompactionConfiguration =
                new DataCompactionConfiguration(new[] {new DataIntervalConfiguration(TimeSpan.FromMinutes(5), TimeSpan.MaxValue), });
        }

        public string StoragePath { get; set; }
        public string LocalSourceName { get; set; }
        public TimeSpan SealTime { get; set; }
        public TimeSpan MaximumDataAge { get; set; }
        public DataCompactionConfiguration CompactionConfiguration { get; set; }
        public RecyclableMemoryStreamManager MemoryStreamManager { get { return memoryStreamManager; } }
	    public bool ShuttingDown { get { return false; } }
    }

    [TestFixture]
    public sealed class DataSetTests
    {
        private const long FirstBucketValue = 3;
        private const long SecondBucketValue = 867;
        private const long ThirdBucketValue = 5309;

        private DimensionSet dimensionSet;
        private DimensionSpecification dimensionSpec;

        private TimeSpan bucketSpan;
        private DateTime firstBucketTimestamp, secondBucketTimestamp, thirdBucketTimestamp;
        private MockSharedDataSetProperties sharedProperties;

        private DataSet<InternalHitCount> dataSet;
        private DataSet<InternalHistogram> histogramDataSet;
        
        [SetUp]
        public void SetUp()
        {
            this.dimensionSet = new DimensionSet(new HashSet<Dimension>());
            this.dimensionSpec = new DimensionSpecification();
            this.sharedProperties = new MockSharedDataSetProperties();
            this.bucketSpan = this.sharedProperties.CompactionConfiguration.Default.Interval;

            this.firstBucketTimestamp = DateTime.UtcNow;
            this.secondBucketTimestamp = this.firstBucketTimestamp + this.bucketSpan;
            this.thirdBucketTimestamp = this.secondBucketTimestamp + this.bucketSpan;
        }

        [TearDown]
        public void TearDown()
        {
            if (this.dataSet != null)
            {
                this.dataSet.Dispose();
                this.dataSet = null;
            }

            if (this.histogramDataSet != null)
            {
                this.histogramDataSet.Dispose();
                this.histogramDataSet = null;
            }
        }

        // For all tests we write sample data as:
        // Bucket 1: 1 increment
        // Bucket 2: Default value
        // Bucket 1: 1 increment
        // Bucket 3: Default value
        // Bucket 1: 1 increment.
        // Depending on configuration we expect various 
        private void WriteSampleData()
        {
            this.dataSet.AddValue(1, this.dimensionSpec, this.firstBucketTimestamp);
            this.dataSet.AddValue(SecondBucketValue, this.dimensionSpec, this.secondBucketTimestamp);
            // Should not be sealed yet as the end time - start time of the newest bucket is not five minutes.
            this.dataSet.AddValue(1, this.dimensionSpec, this.firstBucketTimestamp);
            this.dataSet.AddValue(ThirdBucketValue, this.dimensionSpec, this.thirdBucketTimestamp);
            // As of the third bucket we anticipate that bucket 1 is now sealed.
            this.dataSet.AddValue(1, this.dimensionSpec, this.firstBucketTimestamp);
            this.dataSet.Flush();
        }

        private DataSample GetBucket(DateTime startTime, DateTime endTime)
        {
            var queryResult = dataSet.QueryData(new DimensionSpecification 
                                                {

                                                    {ReservedDimensions.StartTimeDimension, DateTime.MinValue.ToString("o")},
                                                    {ReservedDimensions.EndTimeDimension, DateTime.MaxValue.ToString("o")},
                                                },
                                                new QuerySpecification
                                                {
                                                    Combine = false,
                                                    CrossQueryDimension = null,
                                                    Percentile = double.NaN,
                                                }).ToList();

            foreach (var bucket in queryResult)
            {
                if (bucket.StartTime.ToDateTime() <= startTime && bucket.EndTime.ToDateTime() <= endTime)
                {
                    return bucket;
                }
            }

            return null;
        }

        [Test]
        public void DataBucketsAreNotSealedOrDeletedForZeroTimeSpan()
        {
            this.dataSet = new DataSet<InternalHitCount>("/TestSet", null, this.dimensionSet,
                                                                                    new MockSharedDataSetProperties());

            this.WriteSampleData();

            Assert.AreEqual(FirstBucketValue, this.GetBucket(firstBucketTimestamp, secondBucketTimestamp).HitCount);
            Assert.AreEqual(SecondBucketValue, this.GetBucket(secondBucketTimestamp, thirdBucketTimestamp).HitCount);
            Assert.AreEqual(ThirdBucketValue, this.GetBucket(thirdBucketTimestamp, DateTime.MaxValue).HitCount);
        }

        [Test]
        public void DataBucketsAreSealedAfterSpecifiedElapsedInterval()
        {
            var sealTime = this.bucketSpan;
            this.dataSet = new DataSet<InternalHitCount>("/TestSet", null, this.dimensionSet,
                                                                                    new MockSharedDataSetProperties
                                                                                    {
                                                                                        SealTime = sealTime,
                                                                                    });
            // We asked to seal after one bucket timespan. So the third write to bucket 1 will not actually occur
            // (it will be quietly rejected) and the first bucket value will be down one increment.
            this.WriteSampleData();

            Assert.AreEqual(FirstBucketValue - 1, this.GetBucket(firstBucketTimestamp, secondBucketTimestamp).HitCount);
            Assert.AreEqual(SecondBucketValue, this.GetBucket(secondBucketTimestamp, thirdBucketTimestamp).HitCount);
            Assert.AreEqual(ThirdBucketValue, this.GetBucket(thirdBucketTimestamp, DateTime.MaxValue).HitCount);
        }

        [Test]
        public void DataBucketsAreDeletedAfterSpecifiedElapsedInterval()
        {
            var deleteTime = this.bucketSpan;
            this.dataSet = new DataSet<InternalHitCount>("/TestSet", null,
                                                                                    this.dimensionSet,
                                                                                    new MockSharedDataSetProperties
                                                                                    {
                                                                                        MaximumDataAge = deleteTime,
                                                                                    });

            // We asked to seal after one bucket timespan. So the third write to bucket 1 will not actually occur
            // (it will be quietly rejected) and the first bucket value will be down one increment.
            this.WriteSampleData();

            Assert.IsNull(this.GetBucket(firstBucketTimestamp, secondBucketTimestamp));
            Assert.AreEqual(SecondBucketValue, this.GetBucket(secondBucketTimestamp, thirdBucketTimestamp).HitCount);
            Assert.AreEqual(ThirdBucketValue, this.GetBucket(thirdBucketTimestamp, DateTime.MaxValue).HitCount);
        }

        #region query tests
        private const int DimensionOneCount = 10;
        private const int DimensionTwoCount = 42;
        private const int BucketCount = 60;
        private const int MaxValue = 357;
        private static int[][][] histogramKeys;
        private static Dictionary<long, uint> totalCountBySample;
        
        [Test]
        public void AggregateDataQueryWithoutSplitOrFilterReturnsExpectedResult()
        {
            this.WriteComplexSampleData();
            var samples = this.histogramDataSet.QueryData(new DimensionSpecification(), new QuerySpecification
                                                                                        {
                                                                                            Combine = true,
                                                                                            QueryType = QueryType.Normal,
                                                                                        }).ToList();

            Assert.AreEqual(1, samples.Count);
            var histogram = samples[0];

            uint total = 0;
            foreach (var kvp in histogram.Histogram)
            {
                total += kvp.Value;
                Assert.AreEqual(totalCountBySample[kvp.Key], kvp.Value);
            }
            Assert.AreEqual((uint)(DimensionOneCount * DimensionTwoCount * BucketCount), total);
        }

        [Test]
        public void AggregateDataQueryWithSplitWithoutFilterReturnsExpectedResult()
        {
            this.WriteComplexSampleData();
            var samples = this.histogramDataSet.QueryData(new DimensionSpecification(), new QuerySpecification
                                                                                        {
                                                                                            Combine = true,
                                                                                            CrossQueryDimension = "one",
                                                                                            QueryType = QueryType.Normal,
                                                                                        }).ToList();

            Assert.AreEqual(DimensionOneCount, samples.Count);
            var dims = new HashSet<string>();
            foreach (var histDS in samples)
            {
                var dim1String = histDS.Dimensions["one"];
                Assert.IsFalse(dims.Contains(histDS.Dimensions["one"]));
                dims.Add(dim1String);

                var values = new Dictionary<long, uint>();
                var dim1 = int.Parse(dim1String);
                foreach (var bucket in histogramKeys)
                {
                    foreach (var dim2Value in bucket[dim1])
                    {
                        if (!values.ContainsKey(dim2Value))
                        {
                            values[dim2Value] = 0;
                        }
                        values[dim2Value]++;
                    }
                }

                Assert.AreEqual(histDS.Histogram.Count, values.Count);

                uint total = 0;
                foreach (var kvp in histDS.Histogram)
                {
                    total += histDS.Histogram[kvp.Key];
                    Assert.AreEqual(kvp.Value, values[kvp.Key]);
                }
                Assert.AreEqual((uint)(DimensionTwoCount * BucketCount), total);
            }
            Assert.AreEqual(DimensionOneCount, dims.Count);
        }

        [Test]
        public void AggregateDataQueryWithoutSplitWithFilterReturnsExpectedResult()
        {
            const int dim1 = (DimensionOneCount - 1);
            var dim1String = dim1.ToString();
            this.WriteComplexSampleData();
            var samples = this.histogramDataSet.QueryData(new DimensionSpecification
                                                          {{"one", dim1String}},
                                                          new QuerySpecification
                                                          {
                                                              Combine = true,
                                                              QueryType = QueryType.Normal,
                                                          }).ToList();

            Assert.AreEqual(1, samples.Count);
            var histDS = samples[0];
            var values = new Dictionary<long, uint>();
            foreach (var bucket in histogramKeys)
            {
                foreach (var dim2Value in bucket[dim1])
                {
                    if (!values.ContainsKey(dim2Value))
                    {
                        values[dim2Value] = 0;
                    }
                    values[dim2Value]++;
                }
            }

            Assert.AreEqual(histDS.Histogram.Count, values.Count);

            uint total = 0;
            foreach (var kvp in histDS.Histogram)
            {
                total += histDS.Histogram[kvp.Key];
                Assert.AreEqual(kvp.Value, values[kvp.Key]);
            }
            Assert.AreEqual((uint)(DimensionTwoCount * BucketCount), total);
        }

        public void AggregateDataQueryWithSplitWithFilterReturnsExpectedResult()
        {
            const int dim1 = (DimensionOneCount - 1);
            var dim1String = dim1.ToString();
            this.WriteComplexSampleData();
            var samples = this.histogramDataSet.QueryData(new DimensionSpecification
                                                          {{"one", dim1String}},
                                                          new QuerySpecification
                                                          {
                                                              Combine = true,
                                                              CrossQueryDimension = "two",
                                                              QueryType = QueryType.Normal,
                                                          }).ToList();

            Assert.AreEqual(DimensionTwoCount, samples.Count);

            var dims = new HashSet<string>();
            foreach (var histDS in samples)
            {
                var dim2String = histDS.Dimensions["two"];
                Assert.IsFalse(dims.Contains(dim2String));
                dims.Add(dim2String);
                var dim2 = int.Parse(dim2String);

                var values = new Dictionary<long, uint>();
                foreach (var bucket in histogramKeys)
                {
                    var key = bucket[dim1][dim2];
                    if (!values.ContainsKey(key))
                    {
                        values[key] = 0;
                    }
                    values[key]++;
                }

                uint total = 0;
                Assert.AreEqual(histDS.Histogram.Count, values.Count);
                foreach (var kvp in histDS.Histogram)
                {
                    total += values[kvp.Key];
                    Assert.AreEqual(kvp.Value, values[kvp.Key]);
                }
                Assert.AreEqual((uint)BucketCount, total);
            }
        }

        private void WriteComplexSampleData()
        {
            var rng = new Random();
            histogramKeys = new int[BucketCount][][];
            totalCountBySample = new Dictionary<long, uint>();

            this.dimensionSet = new DimensionSet(new HashSet<Dimension>
                                                 {
                                                     new Dimension("one"),
                                                     new Dimension("two"),
                                                 });

            this.sharedProperties.CompactionConfiguration =
                new DataCompactionConfiguration(new[]
                                         {
                                             new DataIntervalConfiguration(TimeSpan.FromMinutes(1), TimeSpan.MaxValue), 
                                         });
            this.histogramDataSet =
                new DataSet<InternalHistogram>("/AggTest", null,
                                                    this.dimensionSet,
                                                    this.sharedProperties);

            var dimSpec = new DimensionSpecification();
            var startTime = new DateTime(2014, 7, 4, 0, 0, 0, DateTimeKind.Utc); // no DateTimeKind.MERKUH? Sigh.
            for (var i = 0; i < BucketCount; ++i)
            {
                histogramKeys[i] = new int[DimensionOneCount][];

                var ts = startTime +
                         TimeSpan.FromTicks(i * this.sharedProperties.CompactionConfiguration.DefaultBucketTicks);
                for (var d1 = 0; d1 < DimensionOneCount; ++d1)
                {
                    histogramKeys[i][d1] = new int[DimensionTwoCount];

                    dimSpec["one"] = d1.ToString();
                    for (var d2 = 0; d2 < DimensionTwoCount; ++d2)
                    {
                        dimSpec["two"] = d2.ToString();

                        var val = rng.Next(MaxValue);
                        this.histogramDataSet.AddValue(val, dimSpec, ts);

                        histogramKeys[i][d1][d2] = val;
                        if (!totalCountBySample.ContainsKey(val))
                        {
                            totalCountBySample[val] = 0;
                        }
                        totalCountBySample[val]++;
                    }
                }
            }

            this.histogramDataSet.Flush();
        }
        #endregion
    }
}
