// ---------------------------------------------------------------------
// <copyright file="InternalData.cs" company="Microsoft">
//     Copyright 2012 (c) Microsoft Corporation. All Rights Reserved.
//     Information Contained Herein is Proprietary and Confidential.
// </copyright>
// --------------------------------------------------------------------

namespace MetricSystem.Data.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    using MetricSystem.Data;

    using NUnit.Framework;

    [TestFixture]
    public sealed class InternalDataTests
    {
        #region HitCount Tests
        [Test]
        public void HitCountInitialValueIsZero()
        {
            var hitCount = new InternalHitCount();
            Assert.AreEqual((ulong)0, hitCount.HitCount);
        }

        [Test]
        public void HitCountCanChangeCountToAnyValidValue()
        {
            var hitCount = new InternalHitCount();
            hitCount.HitCount++;
            Assert.AreEqual((ulong)1, hitCount.HitCount);

            hitCount.HitCount = 0;
            Assert.AreEqual((ulong)0, hitCount.HitCount);
        }

        [Test]
        public void HitCountCountTypeIsSignedLongInteger()
        {
            var hitCount = new InternalHitCount();
            Assert.AreEqual(typeof(long), hitCount.HitCount.GetType());
        }

        [Test]
        public void HitCountMaximumValueIsSignedLongIntegerMax()
        {
            var hitCount = new InternalHitCount();
            hitCount.HitCount = long.MaxValue;
            Assert.AreEqual(long.MaxValue, hitCount.HitCount);

            ++hitCount.HitCount;
            Assert.IsTrue(hitCount.HitCount < 0);
        }

        [Test]
        public unsafe void HitCountCanReadPersistedData()
        {
            const ulong anyHitCount = 42;
            var hitCount = new InternalHitCount();
            var hitCountBytes = BitConverter.GetBytes(anyHitCount);
            fixed (byte* buf = hitCountBytes)
            {
                hitCount.ReadFromPersistedData(buf, sizeof(long), false);
            }
            Assert.AreEqual(anyHitCount, hitCount.HitCount);
        }

        [Test]
        public unsafe void HitCountReadAddsToCurrentValue()
        {
            const ulong anyHitCount = 42;
            var hitCount = new InternalHitCount();
            hitCount.AddValue(37);
            var hitCountBytes = BitConverter.GetBytes(anyHitCount);
            fixed (byte* buf = hitCountBytes)
            {
                hitCount.ReadFromPersistedData(buf, sizeof(ulong), false);
            }
            Assert.AreEqual(anyHitCount + 37, hitCount.HitCount);
        }

        [Test]
        public void HitCountCanWritePersistedData()
        {
            using (var ms = new MemoryStream())
            {
                const long anyHitCount = 42;
                var hitCount = new InternalHitCount {HitCount = anyHitCount};
                hitCount.WriteToPersistedData(ms);

                Assert.AreEqual(anyHitCount, BitConverter.ToUInt64(ms.GetBuffer(), 0));
            }
        }
        #endregion

        #region Histogram Tests
        [Test]
        public void HistogramInitialContentsAreEmpty()
        {
            var histogram = new InternalHistogram();
            Assert.AreEqual((ulong)0, histogram.SampleCount);
            var d = new Dictionary<long, uint>();
            histogram.UpdateDictionary(d);
            Assert.AreEqual(0, d.Count);
        }

        [Test]
        public void HistogramCountsOccurencesOfAddedValues()
        {
            const int oneTimeValue = 7;
            const int multiTimeValue = 42;
            const uint multiTimeValueCount = 13;

            var histogram = new InternalHistogram();
            histogram.AddValue(oneTimeValue);
            for (var i = 0; i < multiTimeValueCount; ++i)
            {
                histogram.AddValue(multiTimeValue);
            }

            var values = new Dictionary<long, uint>();
            histogram.UpdateDictionary(values);
            Assert.AreEqual((uint)1, values[oneTimeValue]);
            Assert.AreEqual(multiTimeValueCount, values[multiTimeValue]);
            Assert.AreEqual(2, values.Count);
        }

        [Test]
        public void HistogramUpdateDictionaryDoesNotOverwriteContents()
        {
            var initialData = new Dictionary<long, uint>();
            const long initialDataKey = 867;
            const uint initialDataValue = 5309;
            initialData.Add(initialDataKey, initialDataValue);

            const int histogramValue = 33;
            var histogram = new InternalHistogram();
            histogram.AddValue(histogramValue);
            histogram.AddValue(histogramValue);

            histogram.UpdateDictionary(initialData);
            Assert.AreEqual(2, initialData.Count);
            Assert.AreEqual(initialDataValue, initialData[initialDataKey]);
            Assert.AreEqual((uint)2, initialData[histogramValue]);
        }

        [Test]
        public void HistogramMaintainsCountOfAddedSamples()
        {
            const ulong anySampleCount = 42;

            var histogram = new InternalHistogram();
            for (ulong i = 0; i < anySampleCount; ++i)
            {
                histogram.AddValue((long)i);
            }

            Assert.AreEqual(anySampleCount, histogram.SampleCount);
        }

        [Test]
        public void HistogramSupportsLargeNumbersOfValues()
        {
            var histogram = new InternalHistogram();
            ulong expectedSampleCount = 0;

            for (var i = short.MinValue; i < short.MaxValue; ++i)
            {
                histogram.AddValue(i);
                histogram.AddValue(i);
                histogram.AddValue(i);
                expectedSampleCount += 3;
            }
            Assert.AreEqual(expectedSampleCount, histogram.SampleCount);

            var outputData = new Dictionary<long, uint>();
            histogram.UpdateDictionary(outputData);
            for (var i = short.MinValue; i < short.MaxValue; ++i)
            {
                Assert.AreEqual((uint)3, outputData[i]);
            }
        }

        [Test]
        public void HistogramCanReadWhatItWrote()
        {
            var histogram = new InternalHistogram();

            for (long i = 0; i < 10; i++)
            {
                histogram.AddValue(i / 2);
            }

            var histogram2 = new InternalHistogram();

            using (var mem = new MemoryStream())
            {
                histogram.WriteToPersistedData(mem);

                mem.Position = 0;
                using (var buffer = new BufferedValueArray.VariableLengthBufferedValueArray(mem.GetBuffer(), 0, (int)mem.Length))
                {
                    histogram2.MergeFrom(new MultiValueMergeSource(buffer, 0));
                }
            }

            Assert.AreEqual(histogram.SampleCount, histogram2.SampleCount);
            foreach (var kvp in histogram.Data)
            {
                Assert.AreEqual(kvp.Value, histogram2.Data[kvp.Key]);
            }
        }
        #endregion
    }
}
