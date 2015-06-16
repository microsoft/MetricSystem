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
    using System.Threading.Tasks;

    using NUnit.Framework;

    [TestFixture]
    public sealed class DataBucketTests
    {
        private DimensionSet oneDimensionSet;
        private DimensionSet twoDimensionSet;
        private DateTime timestamp;
        private string currentDirectory;
        private DataBucket<InternalHitCount> bucket;
        private MockSharedDataSetProperties properties;

        private const long DefaultBucketTimeSpanTicks = TimeSpan.TicksPerMinute * 5;

        [SetUp]
        public void SetUp()
        {
            this.properties = new MockSharedDataSetProperties();
            this.oneDimensionSet = new DimensionSet(new HashSet<Dimension>
                                                    {
                                                        new Dimension("one"),
                                                    });

            this.twoDimensionSet = new DimensionSet(new HashSet<Dimension>
                                                    {
                                                        new Dimension("one"),
                                                        new Dimension("two")
                                                    });
            this.timestamp = DateTime.UtcNow;
            this.currentDirectory = Directory.GetCurrentDirectory();
            this.RecreateDataBucket();

            if (File.Exists(this.bucket.Filename))
            {
                File.Delete(this.bucket.Filename);
                this.RecreateDataBucket();
            }
        }

        [TearDown]
        public void TearDown()
        {
            if (this.bucket != null)
            {
                var file = this.bucket.Filename;

                this.bucket.Dispose();
                this.bucket = null;

                if (File.Exists(file))
                {
                    File.Delete(file);
                }

            }
        }

        [Test]
        public void CanRetrieveDataFromBucketInMemory()
        {
            this.WriteSomeData();
            this.ValidateData();
        }

        [Test]
        public void WriteToSealedBucketThrowsInvalidOperationException()
        {
            Assert.IsFalse(this.bucket.Sealed);
            this.WriteSomeData();
            this.ValidateData();
            Assert.IsTrue(this.bucket.Sealed);
            Assert.Throws<InvalidOperationException>(this.WriteSomeData);
            this.ValidateData();
        }

        [Test]
        public void IfNoDataIsWrittenPersistingDoesNotCreateFile()
        {
            this.bucket.Persist();
            Assert.IsFalse(File.Exists(this.bucket.Filename));
        }

        [Test]
        public void PersistingFileDoesNotSealBucket()
        {
            this.WriteSomeData();
            this.bucket.Persist();
            Assert.IsFalse(this.bucket.Sealed);
        }

        [Test]
        public void IfDataIsWrittenFileIsPersisted()
        {
            this.WriteSomeData();
            this.bucket.Persist();
            Assert.IsTrue(File.Exists(this.bucket.Filename));
        }

        [Test]
        public void PersistedDataIsLoadedOnDemand()
        {
            this.WriteSomeData();
            this.RecreateFileBackedDataBucket();
            this.ValidateData();
        }

        [Test]
        public void SplitByDimensionWithFiltersWorksProperly()
        {
            var filterableDimension = new DimensionSet(new HashSet<Dimension> {new Dimension("thing"), new Dimension("meat")});
            using (
                var filterableBucket =
                    new DataBucket<InternalHitCount>(filterableDimension, this.timestamp,
                                                                                DefaultBucketTimeSpanTicks,
                                                                                this.currentDirectory,
                                                                                this.properties.MemoryStreamManager))
            {
                var queryDimensions = new DimensionSpecification();
                queryDimensions["thing"] = "thingOne";
                queryDimensions["meat"] = "bacon";
                filterableBucket.AddValue(queryDimensions, 100);

                queryDimensions["thing"] = "thingTwo";
                queryDimensions["meat"] = "pepperoni";
                filterableBucket.AddValue(queryDimensions, 200);
                filterableBucket.Seal();

                // thingOne and thingTwo will match with no filter
                Assert.AreEqual(2,
                                filterableBucket.GetMatchesSplitByDimension(new DimensionSpecification(), "thing")
                                                .Sum(match => match.DataCount));

                // only thingOne matches bacon
                var bestMatchFilter = new DimensionSpecification {{"meat", "bacon"}};
                Assert.AreEqual(1,
                                filterableBucket.GetMatchesSplitByDimension(bestMatchFilter, "thing")
                                                .Sum(match => match.DataCount));
            }
        }

        [Test]
        public void SealAndReleaseAreThreadSafe()
        {
            var filterableDimension = new DimensionSet(new HashSet<Dimension> { new Dimension("thing") });
            using (
                var filterableBucket =
                    new DataBucket<InternalHitCount>(filterableDimension,
                                                                                this.timestamp,
                                                                                DefaultBucketTimeSpanTicks,
                                                                                this.currentDirectory,
                                                                                properties.MemoryStreamManager))
            {


                var allDims = new DimensionSpecification {{"thing", "thing"}};
                Parallel.For(0, 10, (i) => filterableBucket.AddValue(allDims, i));

                Parallel.For(0, 100, (i) =>
                                     {
                                         switch (i % 3)
                                         {
                                         case 0:
                                             foreach (var item in filterableBucket.GetMatches(allDims))
                                             {
                                                 Assert.IsNotNull(item);
                                             }
                                             break;

                                         case 1:
                                             filterableBucket.AddValue(allDims, 11);
                                             break;

                                         case 2:
                                             filterableBucket.ReleaseData();
                                             break;
                                         }
                                     });
            }
        }

        [Test]
        public void KeyConversionProducesExpectedData()
        {
            this.WriteSomeData();
            this.RecreateFileBackedDataBucket(this.oneDimensionSet);
            this.ValidateData(10, 420);
        }

        [Test]
        public void TruncatedFileDataIsNotLoaded()
        {
            this.WriteSomeData();
            this.RecreateFileBackedDataBucket();
            using (var fs = new FileStream(this.bucket.Filename, FileMode.Open, FileAccess.ReadWrite))
            {
                fs.SetLength(fs.Length / 2);
            }

            this.bucket.LoadData();
            Assert.AreEqual(0, this.bucket.GetMatches(new DimensionSpecification()).ToList()[0].DataCount);
        }

        [Test]
        public void CombinedBucketsAreMergedCorrectly()
        {
            var ts1 = new DateTime(2014, 05, 10, 0, 0, 0);
            var ts2 = new DateTime(2014, 05, 10, 0, 5, 0);
            var sharedDimensions = new DimensionSpecification { {"one", "a1"}, {"two", "a2"}};
            var bucket1Dimensions = new DimensionSpecification { {"one", "b1"}, {"two", "b2"}};
            var bucket2Dimensions = new DimensionSpecification { {"one", "c1"}, {"two", "c2"}};

            var bucket1 =
                new DataBucket<InternalHitCount>(new DimensionSet(this.twoDimensionSet), ts1,
                                                                            TimeSpan.FromMinutes(5).Ticks, null,
                                                                            this.properties.MemoryStreamManager);
            bucket1.AddValue(sharedDimensions, 867);
            bucket1.AddValue(bucket1Dimensions, 5309);

            var bucket2 =
                new DataBucket<InternalHitCount>(new DimensionSet(this.twoDimensionSet), ts2,
                                                                            TimeSpan.FromMinutes(5).Ticks, null,
                                                                            this.properties.MemoryStreamManager);
            bucket2.AddValue(sharedDimensions, 867);
            bucket2.AddValue(bucket2Dimensions, 42);

            bucket1.Seal();
            bucket2.Seal();
            var bucket3 =
                new DataBucket<InternalHitCount>(new[] {bucket1, bucket2},
                                                                            new DimensionSet(this.twoDimensionSet), ts1,
                                                                            TimeSpan.FromMinutes(10).Ticks, null,
                                                                            this.properties.MemoryStreamManager);

            var match = bucket3.GetMatches(bucket1Dimensions).First().Data;
            Assert.AreEqual((ulong)5309, match.HitCount);
            match = bucket3.GetMatches(bucket2Dimensions).First().Data;
            Assert.AreEqual((ulong)42, match.HitCount);

            match = bucket3.GetMatches(sharedDimensions).First().Data;
            Assert.AreEqual((ulong)867 * 2, match.HitCount);

            bucket1.Dispose();
            bucket2.Dispose();
            bucket3.Dispose();
        }

        private const long WrittenValue = 42;
        private void WriteSomeData()
        {
            var dimensionValues = new DimensionSpecification();
            for (var i = 0; i < 10; ++i)
            {
                for (var j = 0; j < 10; ++j)
                {
                    dimensionValues["one"] = i.ToString();
                    dimensionValues["two"] = j.ToString();
                    this.bucket.AddValue(dimensionValues, WrittenValue);
                }
            }
        }

        private void ValidateData(ulong expectedPermutations = 100, ulong expectedData = WrittenValue)
        {
            this.bucket.Seal();

            var uniqueKeys = new HashSet<string>();
            var dimensionValues = new DimensionSpecification();
            for (var i = 0; i < 10; ++i)
            {
                for (var j = 0; j < 10; ++j)
                {
                    dimensionValues["one"] = i.ToString();
                    dimensionValues["two"] = j.ToString();
                    var keyString =
                        this.bucket.DimensionSet.KeyToString(this.bucket.DimensionSet.CreateKey(dimensionValues));
                    uniqueKeys.Add(keyString);

                    var matchData = this.bucket.GetMatches(dimensionValues).ToList()[0];
                    Assert.AreEqual(1, matchData.DataCount);
                    Assert.AreEqual(expectedData, matchData.Data.HitCount);
                }
            }
            Assert.AreEqual(expectedPermutations, (ulong)uniqueKeys.Count);
        }

        private void RecreateDataBucket(DimensionSet set = null)
        {
            if (this.bucket != null)
            {
                this.bucket.Dispose();
            }
            this.bucket =
                new DataBucket<InternalHitCount>(
                    new DimensionSet(set ?? this.twoDimensionSet), this.timestamp, DefaultBucketTimeSpanTicks,
                    this.currentDirectory, this.properties.MemoryStreamManager);
        }

        private void RecreateFileBackedDataBucket(DimensionSet set = null)
        {
            if (this.bucket != null)
            {
                this.bucket.Dispose();
            }

            this.bucket =
                new DataBucket<InternalHitCount>(
                    new DimensionSet(set ?? this.twoDimensionSet),
                                        this.bucket.Filename, this.properties.MemoryStreamManager, null);
        }
    }
}
