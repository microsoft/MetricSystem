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
    using System.IO;
    using System.Threading.Tasks;

    using Newtonsoft.Json;

    using NUnit.Framework;

    [TestFixture]
    public sealed class CounterConfigurationTests
    {
        private static CounterConfiguration Deserialize(string json)
        {
            var serializer = new JsonSerializer();
            using (var textReader = new StringReader(json))
            {
                using (var jsonReader = new JsonTextReader(textReader))
                {
                    return serializer.Deserialize<CounterConfiguration>(jsonReader);
                }
            }
        }

        [Test]
        public void CounterWithoutDimensionSetIsOK()
        {
            var config = Deserialize(@"
{""Counters"":
    {""/Test"":{Type:""HitCount""}}
}");
            Assert.IsTrue(config.Validate());
        }

        [Test]
        public void CounterSectionIsRequired()
        {
            try
            {
                Deserialize("{}");
                Assert.Fail();
            }
            catch (JsonException) { }
        }

        [Test]
        public void CounterSectionMayBeEmpty()
        {
            var config = Deserialize(@"{""Counters"":{}}");
            Assert.IsTrue(config.Validate());
        }

        [Test]
        public void CounterNamesWithOnlySpaceAreNotValid()
        {
            var config = Deserialize(@"
{""Counters"":
    {""/  "":{Type:""HitCount""}}
}");
            Assert.IsFalse(config.Validate());
        }

        [Test]
        public void CounterWithUnknownTypeIsNotValid()
        {
            var config = Deserialize(@"
{""Counters"":
    {""/Test"":{""Type"":""zuh??""}}
}");
            Assert.IsFalse(config.Validate());
        }

        [Test]
        public void CounterWithUnknownRoundingIsNotValid()
        {
            var config = Deserialize(@"
{""Counters"":
    {""/Test"":{""Type"":""Histogram"",""Rounding"":""Bloop"",""RoundingFactor"":""5""}}
}");
            Assert.IsFalse(config.Validate());
        }

        [Test]
        public void HistogramCounterWithRoundingButNoFactorIsNotValid()
        {
            var config = Deserialize(@"
{""Counters"":
    {""/Test"":{""Type"":""Histogram"",""Rounding"":""SignificantDigits""}}
}");
            Assert.IsFalse(config.Validate());
        }

        [Test]
        public async Task HistogramCounterWithRoundingAndNonZeroFactorIsValid()
        {
            var config = Deserialize(@"
{""Counters"":
    {""/Test"":{""Type"":""Histogram"",""Rounding"":""SignificantDigits"",""RoundingFactor"":""3""}}
}");
            Assert.IsTrue(config.Validate());
            using (var dm = new DataManager())
            {
                await config.Apply(dm);
                var counter = dm.GetCounter<HistogramCounter>("/Test");
                Assert.AreEqual(CounterRounding.SignificantDigits, counter.Rounding);
                Assert.AreEqual(3, counter.RoundingFactor);
            }

            config = Deserialize(@"
{""Counters"":
    {""/Test"":{""Type"":""Histogram"",""Rounding"":""ByteCount"",""RoundingFactor"":""1024""}}
}");
            Assert.IsTrue(config.Validate());
            using (var dm = new DataManager())
            {
                await config.Apply(dm);
                var counter = dm.GetCounter<HistogramCounter>("/Test");
                Assert.AreEqual(CounterRounding.ByteCount, counter.Rounding);
                Assert.AreEqual(1024, counter.RoundingFactor);
            }
        }

        [Test]
        public void HistogramCounterWithRoundingAndZeroOrNegativeFactorIsInvalid()
        {
            var config = Deserialize(@"
{""Counters"":
    {""/Test"":{""Type"":""Histogram"",""Rounding"":""SignificantDigits"",""RoundingFactor"":""0""}}
}");
            Assert.IsFalse(config.Validate());

            config = Deserialize(@"
{""Counters"":
    {""/Test"":{""Type"":""Histogram"",""Rounding"":""SignificantDigits"",""RoundingFactor"":""-27""}}
}");
            Assert.IsFalse(config.Validate());

            config = Deserialize(@"
{""Counters"":
    {""/Test"":{""Type"":""Histogram"",""Rounding"":""ByteCount"",""RoundingFactor"":""0""}}
}");
            Assert.IsFalse(config.Validate());

            config = Deserialize(@"
{""Counters"":
    {""/Test"":{""Type"":""Histogram"",""Rounding"":""ByteCount"",""RoundingFactor"":""-42""}}
}");
            Assert.IsFalse(config.Validate());
        }

        [Test]
        public async Task CountersAggregateByDefault()
        {
            var config = Deserialize(@"
{""Counters"":
    {""/Test"":{""Type"":""HitCount""}}
}");
            using (var dm = new DataManager())
            {
                await config.Apply(dm);
                var counter = dm.GetCounter<Counter>("/Test");
                Assert.IsTrue(counter.ShouldAggregate());
            }
        }

        [Test]
        public async Task CounterAggregationCanBeDisabled()
        {
            var config = Deserialize(@"
{""Counters"":
    {""/Test"":{""Type"":""HitCount"",""Aggregate"":false}}
}");
            using (var dm = new DataManager())
            {
                await config.Apply(dm);
                var counter = dm.GetCounter<Counter>("/Test");
                Assert.IsFalse(counter.ShouldAggregate());
            }
        }
    }
}
