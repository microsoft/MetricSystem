namespace MetricSystem.Client.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using MetricSystem;
    using MetricSystem.Data;
    using MetricSystem.Utilities;

    using NUnit.Framework;

    [TestFixture]
    public class CounterAggregatorTests
    {
        private const string AnyDimensionName = "anyDim";
        private static readonly DimensionSet DefaultDimensions = new DimensionSet(new HashSet<Dimension>{new Dimension(AnyDimensionName)});
            
        [Test]
        public void CounterAggregatorValidatesArguments()
        {
            Assert.Throws<ArgumentNullException>(() => new CounterAggregator(DefaultDimensions).AddMachineResponse(null));
        }

        [Test]
        public void CounterAggregatorDoesNothingWithoutData()
        {
            var aggregator = new CounterAggregator(DefaultDimensions);
            Assert.IsTrue(!aggregator.Samples.Any());
            Assert.IsTrue(!aggregator.TimeMergedSamples.Any());
        }

        [Test]
        public void CounterAggregatorCanSmashAllBucketsTogether()
        {
            var aggregator = new CounterAggregator(DefaultDimensions);
            aggregator.AddMachineResponse(this.CreateResponse(DateTime.Now, 1, 1, 10));

            var superSamples = aggregator.TimeMergedSamples;
            Assert.IsNotNull(superSamples);
            this.VerifySample(superSamples.First(), 10);
            Assert.AreEqual(1, superSamples.Count());
        }

        [Test]
        public void CounterAggregatorCountsMachinesCorrectly()
        {
            var aggregator = new CounterAggregator(DefaultDimensions);
            var response = this.CreateResponse(DateTime.Now, 1, 1, 10);
            const int MachineCount = 15;

            for (int i = 0; i < MachineCount; i++)
            {
                aggregator.AddMachineResponse(response);
            }

            var superSamples = aggregator.TimeMergedSamples;
            Assert.IsNotNull(superSamples);
            Assert.AreEqual(1, superSamples.Count());
            Assert.AreEqual(MachineCount, (int)superSamples.First().MachineCount);
        }

        [Test]
        public void CounterAggregatorRollsUpMachineCountProperly()
        {
            var aggregator = new CounterAggregator(DefaultDimensions);
            var response = this.CreateResponse(DateTime.Now, 1, 1, 10);
            
            const int ResponseCount = 15;
            const int MachineCountPerResponse = 10;

            // add multiple machines per response
            response.Samples = response.Samples.Select(s =>
                                                       {
                                                           var originalValue = s;
                                                           return new DataSample
                                                               {
                                                                   Dimensions = originalValue.Dimensions,
                                                                   EndTime = originalValue.EndTime,
                                                                   HitCount = originalValue.HitCount,
                                                                   MachineCount = MachineCountPerResponse,
                                                                   Name = originalValue.Name,
                                                                   SampleType = originalValue.SampleType,
                                                                   StartTime = originalValue.StartTime
                                                               };
                                                       }).ToList();

            for (int i = 0; i < ResponseCount; i++)
            {
                aggregator.AddMachineResponse(response);
            }


            var superSamples = aggregator.TimeMergedSamples;
            Assert.IsNotNull(superSamples);
            Assert.AreEqual(1, superSamples.Count());
            Assert.AreEqual(MachineCountPerResponse * ResponseCount, (int)superSamples.First().MachineCount);
        }

        [Test]
        public void CounterAggregatorKeepsDifferentSamplesSeparate()
        {
            // this ensures split by works correctly
            var aggregator = new CounterAggregator(DefaultDimensions);
            aggregator.AddMachineResponse(this.CreateResponse(DateTime.Now, 1, 1, 10, 5));

            var superSamples = aggregator.TimeMergedSamples;
            Assert.IsNotNull(superSamples);
            Assert.AreEqual(5, superSamples.Count());
            superSamples.All(x =>
                             {
                                 this.VerifySample(x, 10);
                                 return true;
                             });
        }

        [Test]
        public void CounterAggregatorCanHandleRangesWhichDoNotOverlap()
        {
            var aggregator = new CounterAggregator(DefaultDimensions);
            aggregator.AddMachineResponse(this.CreateResponse(DateTime.Now, 1, 1, 5));
            aggregator.AddMachineResponse(this.CreateResponse(DateTime.Now.AddDays(-1), 1, 1, 5));

            Assert.AreEqual(10, aggregator.Samples.Count(item =>
                                   {
                                       this.VerifySample(item, 1);
                                       return true;
                                   }));
        }

        [Test]
        public void CounterAggregatorRejectsPerMachinePercentiles()
        {
            var aggregator = new CounterAggregator(DefaultDimensions);

            var response = new CounterQueryResponse
                           {
                               HttpResponseCode = 200,
                               Samples = new List<DataSample>
                                         {
                                             new DataSample
                                             {
                                                 Name = "bob",
                                                 StartTime = DateTime.Now.ToMillisecondTimestamp(),
                                                 EndTime = DateTime.Now.ToMillisecondTimestamp(),
                                                 Dimensions =
                                                     new Dictionary<string, string> {{AnyDimensionName, "tacos"}},
                                                 SampleType = DataSampleType.Percentile,
                                                 Percentile = 40,
                                                 PercentileValue = 11,
                                             }
                                         }
                           };

            Assert.Throws<ArgumentException>(() => aggregator.AddMachineResponse(response));
        }

        [Test]
        public void CounterAggregatorCalculatesPercentileAfterAggregation()
        {
            var aggregator = new CounterAggregator(DefaultDimensions);

            var response = new CounterQueryResponse
            {
                HttpResponseCode = 200,
                Samples = new List<DataSample>
                                         {
                                             new DataSample
                                             {
                                                 Name = "bob",
                                                 StartTime = DateTime.Now.ToMillisecondTimestamp(),
                                                 EndTime = DateTime.Now.ToMillisecondTimestamp(),
                                                 Dimensions =
                                                     new Dictionary<string, string> {{AnyDimensionName, "tacos"}},
                                                 SampleType = DataSampleType.Histogram,
                                                 Histogram = new Dictionary<long, uint> {{1,1},{2,1},{3,1},{4,1},{5,1},{6,1},{7,1},{8,1},{9,1},{10,1}}
                                             }
                                         }
            };

            // by default, we do not apply percentile filtering
            aggregator.AddMachineResponse(response);
            var defaultValue = aggregator.Samples.First();
            Assert.AreEqual(DataSampleType.Histogram, defaultValue.SampleType);

            // now that the client asked for filtering, we calculate the 99.999% percentile (should be the max value from earlier)
            aggregator.ApplyPercentileCalculationAggregation(new Dictionary<string, string> {{"Percentile", "99.999"}});
            var aggregatedValue = aggregator.Samples.First();
            Assert.AreEqual(DataSampleType.Percentile, aggregatedValue.SampleType);
            Assert.AreEqual(10, aggregatedValue.PercentileValue);
        }

        [Test]
        public void CounterAggregatorFiltersQueryParametersProperly()
        {
            var aggregator = new CounterAggregator(DefaultDimensions);

            Assert.IsEmpty(aggregator.ApplyPercentileCalculationAggregation(null));
            Assert.IsEmpty(aggregator.ApplyPercentileCalculationAggregation(new Dictionary<string, string>()));
            Assert.IsEmpty(aggregator.ApplyPercentileCalculationAggregation(new Dictionary<string, string> {{"PERCENTILE", "50.0"}}));
            Assert.IsEmpty(aggregator.ApplyPercentileCalculationAggregation(new Dictionary<string, string> { { "percentile", "50.0" } }));
            Assert.IsEmpty(aggregator.ApplyPercentileCalculationAggregation(new Dictionary<string, string> { { "Percentile", "50.0" } }));
            Assert.IsEmpty(aggregator.ApplyPercentileCalculationAggregation(new Dictionary<string, string> { { "PErcenTILe", "50.0" } }));

            Assert.AreEqual(1, aggregator.ApplyPercentileCalculationAggregation(new Dictionary<string, string> { { "percentile", "MAX" } }).Count());
            Assert.AreEqual(1, aggregator.ApplyPercentileCalculationAggregation(new Dictionary<string, string> { { "percentile", "Average" } }).Count());
            Assert.AreEqual(1, aggregator.ApplyPercentileCalculationAggregation(new Dictionary<string, string> { { "tacos", "delicious" } }).Count());
            Assert.AreEqual(1, aggregator.ApplyPercentileCalculationAggregation(new Dictionary<string, string> { { "percentile", "i am a key tree!" } }).Count());
        }

        [Test]
        public void CounterAggregatorCanHandleRangesWhichAlwaysOverlap()
        {
            var aggregator = new CounterAggregator(DefaultDimensions);

            // 10 minute buckets, staggered by 2 minute increments. Should 100% overlap
            aggregator.AddMachineResponse(this.CreateResponse(DateTime.Now, 10, 2, 10));

            Assert.AreEqual(1, aggregator.Samples.Count(item =>
                                                        {
                                                            this.VerifySample(item, 10);
                                                            return true;
                                                        }));
        }

        [Test]
        public void CounterAggregatorCanHandleSupersetRange()
        {
            var aggregator = new CounterAggregator(DefaultDimensions);

            aggregator.AddMachineResponse(this.CreateResponse(DateTime.Now, 1, 1, 9));

            // super bucket
            aggregator.AddMachineResponse(this.CreateResponse(DateTime.Now.AddHours(-1), 600, 10, 1));
            Assert.AreEqual(1, aggregator.Samples.Count(item =>
            {
                this.VerifySample(item, 10);
                return true;
            }));
        }

        [Test]
        public void CounterAggregatorMergesCaseInsenstively()
        {
            var aggregator = new CounterAggregator(DefaultDimensions);
            var now = DateTime.Now;
            aggregator.AddMachineResponse(this.CreateResponse(now, 1, 0, 1, 1, "taco"));
            aggregator.AddMachineResponse(this.CreateResponse(now, 1, 0, 1, 1, "TACO"));

            Assert.AreEqual(1, aggregator.Samples.Count());
        }

        private void VerifySample(DataSample sample, ulong expectedHitCount)
        {
            Assert.AreEqual(expectedHitCount, sample.HitCount);
        }

        private CounterQueryResponse CreateResponse(DateTime startTime, int bucketTimeInMinutes, int deltaBetweenBucketStarts, int numBuckets, int uniqueDimensions = 1, string dimensionValue = null)
        {
            var response = new CounterQueryResponse {Samples = new List<DataSample>()};

            var bucketStart = startTime;
            for (int dim = 0; dim < uniqueDimensions; dim++)
            {
                for (int i = 0; i < numBuckets; i++)
                {
                    response.Samples.Add(
                        new DataSample
                        {
                            HitCount = 1,
                            Dimensions = new Dictionary<string, string>{{AnyDimensionName, dimensionValue ?? dim.ToString()}},
                            SampleType = DataSampleType.HitCount,
                            StartTime = bucketStart.ToMillisecondTimestamp(),
                            EndTime =
                                bucketStart.AddMinutes(bucketTimeInMinutes)
                                .ToMillisecondTimestamp()
                        });

                    bucketStart = bucketStart.AddMinutes(deltaBetweenBucketStarts);
                }
            }
            return response;
        }
    }
}

