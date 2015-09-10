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

namespace MetricSystem.Utilities.UnitTests
{
    using System;
    using System.Collections.Generic;

    using NUnit.Framework;

    [TestFixture]
    public class HistogramExtensionMethodTests
    {
        [SetUp]
        public void SetUp()
        {
            this.histogram.Clear();
        }

        private readonly Dictionary<long, uint> histogram = new Dictionary<long, uint>();

        private void InitializeHistogramWithUniformValues(int min, int max)
        {
            for (var i = min; i <= max; i++)
            {
                this.histogram[i] = 1;
            }
        }

        private static void VerifyThrows<T>(Action a) where T : Exception
        {
            try
            {
                a();
                Assert.Fail("Should have thrown. Did not. Enter failman: " + typeof(T));
            }
            catch (T)
            {
                //hooray
            }
            catch (Exception e)
            {
                Assert.AreEqual(typeof(T), e.GetType());
            }
        }

        [Test]
        public void AverageAvoidsOverflow()
        {
            this.histogram[int.MaxValue / 2] = 10;
            this.histogram[int.MaxValue / 3] = 1;

            var avg = this.histogram.GetAverageValue();
            Assert.IsTrue(avg > 0.0);

            this.histogram.Clear();
            this.histogram[int.MaxValue] = 100;
            Assert.AreEqual(int.MaxValue, this.histogram.GetAverageValue());
        }

        [Test]
        public void AverageIsCorrectForUniformDistribution()
        {
            // 55 / 11 = 5.0
            this.InitializeHistogramWithUniformValues(0, 10);
            Assert.AreEqual(5.0, this.histogram.GetAverageValue());
        }

        [Test]
        public void AverageIsCorrectWithUnevenDistribution()
        {
            this.histogram[100] = 100;
            this.histogram[0] = 100;
            Assert.AreEqual(50, this.histogram.GetAverageValue());
        }

        [Test]
        public void EmptyHistogramsAreHandledCorrectly()
        {
            Assert.AreEqual(0, this.histogram.GetMaximumValue());
            Assert.AreEqual(0, this.histogram.GetMinimumValue());
            Assert.AreEqual(0, this.histogram.GetAverageValue());
            Assert.AreEqual(0, this.histogram.GetValueAtPercentile(12));
            Assert.AreEqual(0, this.histogram.GetValueAtPercentile(99.999));
        }

        [Test]
        public void MaximumCalculatedCorrectly()
        {
            this.InitializeHistogramWithUniformValues(100, 10000);
            Assert.AreEqual(10000, this.histogram.GetMaximumValue());
        }

        [Test]
        public void MethodsCheckForNull()
        {
            Assert.AreEqual(0, (null as IDictionary<long, uint>).GetValueAtPercentile(10));
            Assert.AreEqual(0, (null as IDictionary<long, uint>).GetAverageValue(10));
            Assert.AreEqual(0, (null as IDictionary<long, uint>).GetMaximumValue());
            Assert.AreEqual(0, (null as IDictionary<long, uint>).GetMinimumValue());
            Assert.AreEqual(0, (null as IDictionary<long, uint>).GetValueAtPercentile(10, 10));
        }

        [Test]
        public void MinimumCalculatedCorrectly()
        {
            this.InitializeHistogramWithUniformValues(100, 10000);
            Assert.AreEqual(100, this.histogram.GetMinimumValue());
        }

        [Test]
        public void PercentileCalculatedCorrectlyForUnevenDistribution()
        {
            this.histogram[0] = 999;
            this.histogram[8675309] = 1;

            Assert.AreEqual(0, this.histogram.GetValueAtPercentile(44));
            Assert.AreEqual(0, this.histogram.GetValueAtPercentile(88));
            Assert.AreEqual(0, this.histogram.GetValueAtPercentile(99));
            Assert.AreEqual(0, this.histogram.GetValueAtPercentile(99.9));
            Assert.AreEqual(8675309, this.histogram.GetValueAtPercentile(99.99));
            Assert.AreEqual(8675309, this.histogram.GetValueAtPercentile(100));
        }

        [Test]
        public void PercentileCalculatedCorrectlyForUniformDistribution()
        {
            this.InitializeHistogramWithUniformValues(1, 100);
            for (var i = 1; i < 100; i++)
            {
                Assert.AreEqual(i, this.histogram.GetValueAtPercentile(i));
            }
        }

        [Test]
        public void PercentileIsCalculatedCorrectlyAtDecimalPercentiles()
        {
            this.InitializeHistogramWithUniformValues(1, 1000);
            Assert.AreEqual(333, this.histogram.GetValueAtPercentile(33.3));
            Assert.AreEqual(666, this.histogram.GetValueAtPercentile(66.6));
            Assert.AreEqual(999, this.histogram.GetValueAtPercentile(99.9));
        }

        [Test]
        public void PercentilesAreValidated()
        {
            VerifyThrows<ArgumentException>(() => this.histogram.GetValueAtPercentile(200));
        }
    }
}
