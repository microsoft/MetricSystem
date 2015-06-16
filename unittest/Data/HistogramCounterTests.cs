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
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using NUnit.Framework;

    [TestFixture]
    public sealed class HistogramCounterTests
    {
        private DataManager dataManager;
        private HistogramCounter counter;
        private readonly DimensionSpecification dims = new DimensionSpecification();

        [SetUp]
        public void SetUp()
        {
            this.dataManager = new DataManager();
            this.counter = this.dataManager.CreateHistogramCounter("/TestCounter", DimensionSet.Empty).Result;
        }

        [TearDown]
        public void TearDown()
        {
            this.counter = null;
            this.dataManager.Dispose();
        }

        [Test]
        public void OnlyValidRoundingTypesAreHonored()
        {
            Assert.DoesNotThrow(() => this.counter.Rounding = CounterRounding.None);
            Assert.DoesNotThrow(() => this.counter.Rounding = CounterRounding.ByteCount);
            Assert.DoesNotThrow(() => this.counter.Rounding = CounterRounding.SignificantDigits);
            Assert.Throws<ArgumentException>(() => this.counter.Rounding = CounterRounding.Unknown);
            Assert.Throws<ArgumentException>(() => this.counter.Rounding = CounterRounding.None - 1);
        }

        [Test]
        public void RoundingFactorMustBeNonNegative()
        {
            Assert.DoesNotThrow(() => this.counter.RoundingFactor = 0);
            Assert.DoesNotThrow(() => this.counter.RoundingFactor = long.MaxValue);
            Assert.Throws<ArgumentException>(() => this.counter.RoundingFactor = long.MinValue);
        }

        [Test]
        public void SignificantDigitRoundingOccursAtSpecifiedMinimum()
        {
            this.counter.Rounding = CounterRounding.SignificantDigits;
            this.counter.RoundingFactor = 2;

            this.counter.AddValue(7, this.dims);
            this.counter.AddValue(42, this.dims);
            this.counter.AddValue(867, this.dims);
            this.counter.AddValue(5309, this.dims);
            this.counter.DataSet.Flush();

            var hist = this.counter.Query(this.dims).First();
            Assert.AreEqual(4, hist.Histogram.Count);
            Assert.IsTrue(hist.Histogram.ContainsKey(7));
            Assert.IsTrue(hist.Histogram.ContainsKey(42));
            Assert.IsTrue(hist.Histogram.ContainsKey(870));
            Assert.IsTrue(hist.Histogram.ContainsKey(5300));

            this.counter.RoundingFactor = 4; // changing this will allow new keys to be added at full fidelity.
            this.counter.AddValue(867, this.dims);
            this.counter.AddValue(5309, this.dims);
            this.counter.AddValue(31337, this.dims);
            this.counter.DataSet.Flush();

            hist = this.counter.Query(this.dims).First();
            Assert.AreEqual(7, hist.Histogram.Count);
            Assert.IsTrue(hist.Histogram.ContainsKey(7));
            Assert.IsTrue(hist.Histogram.ContainsKey(42));
            Assert.IsTrue(hist.Histogram.ContainsKey(867));
            Assert.IsTrue(hist.Histogram.ContainsKey(870));
            Assert.IsTrue(hist.Histogram.ContainsKey(5300));
            Assert.IsTrue(hist.Histogram.ContainsKey(5309));
            Assert.IsTrue(hist.Histogram.ContainsKey(31340));
        }

        [Test]
        public void ByteCountRoundingOccursAtSpecificThreshold()
        {
            this.counter.Rounding = CounterRounding.ByteCount;
            this.counter.RoundingFactor = 1024; // 1KB

            this.counter.AddValue(7, this.dims);
            this.counter.AddValue(42, this.dims);
            this.counter.AddValue(867, this.dims);
            this.counter.AddValue(5309, this.dims);
            this.counter.DataSet.Flush();

            var hist = this.counter.Query(this.dims).First();
            Assert.AreEqual(4, hist.Histogram.Count);
            Assert.IsTrue(hist.Histogram.ContainsKey(7));
            Assert.IsTrue(hist.Histogram.ContainsKey(42));
            Assert.IsTrue(hist.Histogram.ContainsKey(867));
            Assert.IsTrue(hist.Histogram.ContainsKey(5120));

            this.counter.RoundingFactor = 65536; // changing this to round on >64KB at 64KB boundary. 
            this.counter.AddValue(5309, this.dims);
            this.counter.AddValue(65536 + 1024, this.dims); // should be 65536
            this.counter.AddValue(65536 + 32768, this.dims); // should be 131072
            this.counter.DataSet.Flush();

            hist = this.counter.Query(this.dims).First();
            Assert.AreEqual(7, hist.Histogram.Count);
            Assert.IsTrue(hist.Histogram.ContainsKey(7));
            Assert.IsTrue(hist.Histogram.ContainsKey(42));
            Assert.IsTrue(hist.Histogram.ContainsKey(867));
            Assert.IsTrue(hist.Histogram.ContainsKey(5120));
            Assert.IsTrue(hist.Histogram.ContainsKey(5309));
            Assert.IsTrue(hist.Histogram.ContainsKey(65536));
            Assert.IsTrue(hist.Histogram.ContainsKey(131072));
        }
    }
}
