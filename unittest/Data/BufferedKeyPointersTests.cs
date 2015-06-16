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
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using NUnit.Framework;

    using TestBufferedKeyedData = BufferedKeyedData<long>;

    [TestFixture]
    public sealed class BufferedKeyPointersTests
    {
        private Random rng;

        private static TestBufferedKeyedData GetTestData(int keyCount, DimensionSet dimensionSet)
        {
            return
                new TestBufferedKeyedData(
                    new byte[TestBufferedKeyedData.GetBufferSizeForKeyCount(keyCount, dimensionSet)],
                    0, 0, dimensionSet, true);
        }

        [SetUp]
        public void SetUp()
        {
            this.rng = new Random(8675309);
        }

        [Test]
        public void DataMayBeEmpty()
        {
            using (var data = new TestBufferedKeyedData(null, 0, 0, DimensionSet.Empty))
            {
                foreach (var kvp in data)
                {
                    Assert.Fail();
                }
            }
        }

        [Test]
        public unsafe void ThreadedWritesAreSafe()
        {
            const int writeCount = 1000000;
            var dimSet = DimensionSetTests.CreateDimensionSet(1);
            using (var data = GetTestData(writeCount, dimSet))
            {
                Parallel.For(0, writeCount,
                             i =>
                             {
                                 var key = (uint)i;
                                 Assert.IsTrue(data.TryWrite(&key, i));
                             });

                data.Seal();
                var seen = new HashSet<long>();
                foreach (var kvp in data)
                {
                    Assert.AreEqual(kvp.Key[0], kvp.Value);
                    Assert.IsFalse(seen.Contains(kvp.Value));
                    seen.Add(kvp.Value);
                }

                Assert.AreEqual(writeCount, seen.Count);
            }
        }

        [Test]
        public unsafe void CanReadSerializedData()
        {
            const int writeCount = 1000000;
            var dimSet = DimensionSetTests.CreateDimensionSet(1);
            var newStream = new MemoryStream();
            using (var data = GetTestData(writeCount, dimSet))
            {
                for (var i = 0; i < writeCount; ++i)
                {
                    dimSet.dimensions[0].StringToIndex(i.ToString());
                }
                Parallel.For(0, writeCount,
                             i =>
                             {
                                 var key = (uint)i;
                                 Assert.IsTrue(data.TryWrite(&key, i));
                             });

                data.Seal();

                data.Serialize(newStream);
            }
            using (var data = new TestBufferedKeyedData(newStream.GetBuffer(), 0, (int)newStream.Length, dimSet))
            {
                var seen = new HashSet<long>();
                foreach (var kvp in data)
                {
                    Assert.AreEqual(kvp.Key[0], kvp.Value);
                    Assert.IsFalse(seen.Contains(kvp.Value));
                    seen.Add(kvp.Value);
                }

                Assert.AreEqual(writeCount, seen.Count);
            }
        }

        #region sort
        [Test]
        public void MayNotSortWritableData()
        {
            using (var set = GetTestData(1, DimensionSet.Empty))
            {
                    Assert.Throws<NotSupportedException>(set.Sort);
            }
        }

        [Test]
        public unsafe void CanWriteMultipleValuesToZeroDimensionBuffer()
        {
            using (var data = GetTestData(4, DimensionSet.Empty))
            {
                Assert.IsTrue(data.TryWrite(null, 867));
                Assert.IsTrue(data.TryWrite(null, 5309));
                Assert.IsTrue(data.TryWrite(null, 42));
                Assert.IsTrue(data.TryWrite(null, 0));
                data.Seal();

                Assert.AreEqual(4, data.Count);
                Assert.AreEqual(867, data.ElementAt(0).Value);
                Assert.AreEqual(5309, data.ElementAt(1).Value);
                Assert.AreEqual(42, data.ElementAt(2).Value);
                Assert.AreEqual(0, data.ElementAt(3).Value);
            }
        }

        [Test]
        public unsafe void SortOfZeroDimensionBufferIsStable()
        {
            using (var data = GetTestData(4, DimensionSet.Empty))
            {
                Assert.IsTrue(data.TryWrite(null, 867));
                Assert.IsTrue(data.TryWrite(null, 5309));
                Assert.IsTrue(data.TryWrite(null, 42));
                Assert.IsTrue(data.TryWrite(null, 0));
                data.Seal();

                data.Sort();
                Assert.AreEqual(4, data.Count);
                Assert.AreEqual(867, data.ElementAt(0).Value);
                Assert.AreEqual(5309, data.ElementAt(1).Value);
                Assert.AreEqual(42, data.ElementAt(2).Value);
                Assert.AreEqual(0, data.ElementAt(3).Value);
            }
        }

        [Test]
        public unsafe void CanWriteMultipleKeysWithSameValue()
        {
            const int sameKeyCount = 4;
            var dimSet = DimensionSetTests.CreateDimensionSet(1);
            using (var data = GetTestData(sameKeyCount * 2, dimSet))
            {
                uint otherKey = 0;
                uint staticKey = 867; // cannot be const (can't take address)
                for (var i = 0; i < sameKeyCount; ++i)
                {
                    dimSet.dimensions[0].StringToIndex(otherKey.ToString());
                    Assert.IsTrue(data.TryWrite(&otherKey, 42));
                    ++otherKey;

                    dimSet.dimensions[0].StringToIndex(staticKey.ToString());
                    Assert.IsTrue(data.TryWrite(&staticKey, 5309));
                }
                data.Seal();
                Assert.AreEqual(sameKeyCount * 2, data.Count);

                otherKey = 0;
                var count = 0;
                var dupeCount = 0;
                foreach (var kvp in data)
                {
                    if (count % 2 == 0)
                    {
                        Assert.AreEqual(otherKey, kvp.Key.Values[0]);
                        Assert.AreEqual(42, kvp.Value);
                        ++otherKey;
                    }
                    else
                    {
                        Assert.AreEqual(staticKey, kvp.Key.Values[0]);
                        Assert.AreEqual(5309, kvp.Value);
                        ++dupeCount;
                    }
                    ++count;
                }

                Assert.AreEqual(sameKeyCount, dupeCount);
            }
        }

        [Test]
        public void CanSortReversedThreeDepthBuffer()
        {
            const int elementsPerDepth = 100;
            using (var data = this.Generate(new [] {elementsPerDepth, elementsPerDepth, elementsPerDepth}, false))
            {
                Assert.AreEqual((elementsPerDepth * elementsPerDepth * elementsPerDepth), data.Count);

                // Verify current ordering is backwards.
                Key currentKey = null;
                var currentValue = long.MaxValue;
                foreach (var kvp in data)
                {
                    Assert.IsTrue(currentKey == null || currentKey > kvp.Key);
                    currentKey = kvp.Key.Clone() as Key;
                    Assert.IsTrue(currentValue == long.MaxValue || currentValue < kvp.Value);
                    currentValue = kvp.Value;
                }

                BenchmarkSort("reverse", data);

                currentKey = null;
                currentValue = uint.MaxValue;
                foreach (var kvp in data)
                {
                    Assert.IsTrue(currentKey == null || currentKey < kvp.Key);
                    currentKey = kvp.Key.Clone() as Key;
                    Assert.IsTrue(currentValue > kvp.Value);
                    currentValue = kvp.Value;
                }
            }
        }

        [Test]
        public void CanSortRandomThreeDepthBuffer()
        {
            const int elementsPerDepth = 100;
            using (var data = this.Generate(new [] {elementsPerDepth, elementsPerDepth, elementsPerDepth}, true))
            {
                Assert.AreEqual((elementsPerDepth * elementsPerDepth * elementsPerDepth), data.Count);

                BenchmarkSort("random", data);

                Key currentKey = null;
                foreach (var kvp in data)
                {
                    Assert.IsTrue(currentKey == null || currentKey <= kvp.Key);
                    currentKey = kvp.Key.Clone() as Key;
                }
            }
        }

        [Test]
        public void SortIsStable()
        {
            var ms = new MemoryStream();
            using (var bw = new BinaryWriter(ms, Encoding.Default, true))
            {
                bw.Write((uint)867);
                bw.Write((uint)5310);
                bw.Write((uint)42);
                bw.Write((uint)0);
                bw.Write((uint)867);
                bw.Write((uint)5309);
            }

            using (var data =
                new BufferedKeyedData<uint>(ms.GetBuffer(), 0, (int)ms.Length, new DimensionSet(new HashSet<Dimension> {new Dimension("dummy")})))
            {
                data.Sort();
                Assert.AreEqual(3, data.Count);

                var pairs =
                    (from pair in data select new KeyValuePair<Key, long>(pair.Key.Clone() as Key, pair.Value)).ToList();
                Assert.AreEqual((uint)42, pairs[0].Key.Values[0]);
                Assert.AreEqual((uint)0, pairs[0].Value);
                Assert.AreEqual((uint)867, pairs[1].Key.Values[0]);
                Assert.AreEqual((uint)5310, pairs[1].Value);
                Assert.AreEqual((uint)867, pairs[2].Key.Values[0]);
                Assert.AreEqual((uint)5309, pairs[2].Value);
            }
        }

        #region sort benchmarking
        private static void BenchmarkSort(string type, TestBufferedKeyedData data)
        {
            var sw = new Stopwatch();
            sw.Start();
            data.Sort();
            Trace.TraceInformation("Sorted {0} {1} in {2}ms", data.Count, type, sw.ElapsedMilliseconds);
        }

        private unsafe void WriteLevel(TestBufferedKeyedData data, bool random, int[] levels, int current, Key key, ref uint count)
        {
            ++current;
            if (current == levels.Length)
            {
                fixed (uint* keyData = key.Values)
                {
                    Assert.IsTrue(data.TryWrite(keyData, count++));
                }

                return;
            }

            for (var i = levels[current]; i > 0; --i)
            {
                var currentDimension = data.DimensionSet.dimensions[current];
                // Need to ensure index values are forward-ordered for this dimension when generating reverse-ordered
                // data.
                if (i == levels[current] && !random)
                {
                    for (var indexValue = 1; indexValue <= i; ++indexValue)
                    {
                        currentDimension.StringToIndex(indexValue.ToString(CultureInfo.InvariantCulture));
                    }
                }

                var value = (random ? this.rng.Next(levels[current]) : i);
                key[current] = currentDimension.StringToIndex(value.ToString(CultureInfo.InvariantCulture));
                WriteLevel(data, random, levels, current, key, ref count);
            }
        }

        private TestBufferedKeyedData Generate(int[] levels, bool random)
        {
            var dims = DimensionSetTests.CreateDimensionSet(levels.Length);
            int totalKeys = 1;
            for (var i = 0; i < levels.Length; ++i)
            {
                totalKeys *= levels[i];
            }

            var data = GetTestData(totalKeys, dims);

            uint count = 0;
            var key = new Key(new uint[levels.Length]);
            this.WriteLevel(data, random, levels, -1, key, ref count);
            Assert.AreEqual(totalKeys, data.Count);
            data.Seal();

            return data;
        }

        [Ignore, Category("QTestSkip")]
        [Test]
        public void Benchmark()
        {
            using (var set = this.Generate(new[] {1000, 100, 20}, true))
            {
                BenchmarkSort("random-triple", set);
            }

            for (var i = 10; i < 10000000; i *= 10)
            {
                using (var set = this.Generate(new[] {i}, false))
                {
                    BenchmarkSort("reverse-single", set);
                }
            }

            for (var i = 10; i < 10000000; i *= 10)
            {
                using (var set = this.Generate(new[] {i}, true))
                {
                    BenchmarkSort("random-single", set);
                }
            }
        }
        #endregion
        #endregion

        #region convert
        [Test]
        public void MayNotConvertWritableSet()
        {
            using (var data = GetTestData(1, DimensionSet.Empty))
            {
                Assert.Throws<NotSupportedException>(() => data.Convert(DimensionSet.Empty));
            }
        }

        [Test]
        public void DataMayOnlyBeConvertedOnce()
        {
            var dimSet1 = new DimensionSet(new HashSet<Dimension> {new Dimension("one")});
            var dimSet2 = new DimensionSet(new HashSet<Dimension> {new Dimension("one")});

            var conversions = new[] {new[] {dimSet1, dimSet1}, new[] {dimSet1, dimSet2}, new[] {dimSet2, dimSet1}};

            foreach (var pair in conversions)
            {
                using (var data = GetTestData(1, pair[0]))
                {
                    data.Seal();
                    data.Convert(pair[1]);
                    Assert.Throws<NotSupportedException>(() => data.Convert(pair[0]));
                    Assert.Throws<NotSupportedException>(() => data.Convert(pair[1]));
                }
            }
        }

        [Test]
        public unsafe void CanConvertFromZeroToMultipleDimensions()
        {
            for (var i = 1; i < 10; ++i)
            {
                var dimSet = DimensionSetTests.CreateDimensionSet(i);

                const uint valueCount = 100;
                using (var data = GetTestData((int)valueCount, DimensionSet.Empty))
                {
                    for (uint c = 0; c < valueCount; ++c)
                    {
                        Assert.IsTrue(data.TryWrite(null, c));
                    }
                    data.Seal();
                    data.Convert(dimSet);

                    uint currentCount = 0;
                    var wildcardKey = Key.GetWildcardKey(dimSet);
                    foreach (var kvp in data)
                    {
                        Assert.AreEqual(wildcardKey, kvp.Key);
                        Assert.AreEqual(currentCount, kvp.Value);
                        ++currentCount;
                    }
                }
            }
        }

        [Test]
        public void CanConvertFromMultipleToZeroDimensions()
        {
            const uint valueCount = 1000;
            var wildcardKey = Key.GetWildcardKey(DimensionSet.Empty);

            foreach (var levels in new[] {new[] {1000}, new[] {100, 10}, new[] {10, 10, 10}})
            {
                using (var set = this.Generate(levels, true))
                {
                    set.Convert(DimensionSet.Empty);

                    uint count = 0;
                    foreach (var kvp in set)
                    {
                        Assert.AreEqual(wildcardKey, kvp.Key);
                        Assert.AreEqual(count, kvp.Value);
                        ++count;
                    }

                    Assert.AreEqual(valueCount, count);
                }
            }
        }

        [Test]
        public void CanConvertBufferWithIdenticalSet()
        {
            const int elementsPerDepth = 100;
            using (var set = this.Generate(new [] {elementsPerDepth, elementsPerDepth, elementsPerDepth}, true))
            {
                var dimensionSet = set.DimensionSet;

                set.Convert(dimensionSet);
                Assert.AreSame(dimensionSet, set.DimensionSet);
                set.Validate();
            }
        }

        [Test]
        public void CanConvertBufferWithSameSizeSet()
        {
            const int elementsPerDepth = 100;
            using (var set = this.Generate(new [] {elementsPerDepth, elementsPerDepth, elementsPerDepth}, true))
            {
                var originalDimSet = set.DimensionSet;
                var newDimSet = new DimensionSet(originalDimSet);
                var originalKeys = new List<Key>(set.Select(kvp => kvp.Key.Clone() as Key));

                set.Convert(newDimSet);
                Assert.AreSame(newDimSet, set.DimensionSet);

                int currentKey = 0;
                foreach (var kvp in set)
                {
                    for (var i = 0; i < newDimSet.dimensions.Length; ++i)
                    {
                        var newDimension = newDimSet.dimensions[i];
                        var originalDimension =
                            (from d in originalDimSet.dimensions where d.Equals(newDimension) select d).First();
                        var originalDimIndex = Array.IndexOf(originalDimSet.dimensions, originalDimension);
                        var originalKey = originalKeys[currentKey];

                        Assert.AreEqual(originalDimension.IndexToString(originalKey[originalDimIndex]),
                                        newDimension.IndexToString(kvp.Key[i]));
                    }

                    ++currentKey;
                }
                Assert.AreEqual(originalKeys.Count, currentKey);
            }
        }

        [Test]
        public void CanConvertBufferToSmallerDimensionSet()
        {
            const int elementsPerDepth = 100;
            using (var set = this.Generate(new [] {elementsPerDepth, elementsPerDepth, elementsPerDepth}, true))
            {
                var originalDimSet = set.DimensionSet;
                var newDimSet =
                    new DimensionSet(new HashSet<Dimension> {originalDimSet.dimensions[0], originalDimSet.dimensions[1]});
                var originalKeys = new List<Key>(set.Select(kvp => kvp.Key.Clone() as Key));

                set.Convert(newDimSet);
                Assert.AreSame(newDimSet, set.DimensionSet);

                int currentKey = 0;
                foreach (var kvp in set)
                {
                    for (var i = 0; i < newDimSet.dimensions.Length; ++i)
                    {
                        var originalKey = originalKeys[currentKey];

                        var newDimension = newDimSet.dimensions[i];
                        var originalDimension =
                            (from d in originalDimSet.dimensions where d.Equals(newDimension) select d).FirstOrDefault();
                        if (originalDimension != null)
                        {
                            var originalDimIndex = Array.IndexOf(originalDimSet.dimensions, originalDimension);
                            Assert.AreEqual(originalDimension.IndexToString(originalKey[originalDimIndex]),
                                            newDimension.IndexToString(kvp.Key[i]));
                        }
                    }

                    ++currentKey;
                }
                Assert.AreEqual(originalKeys.Count, currentKey);
            }
        }

        [Test]
        public void CanConvertBufferToLargerDimensionSet()
        {
            const int elementsPerDepth = 100;
            using (var set = this.Generate(new [] {elementsPerDepth, elementsPerDepth, elementsPerDepth}, true))
            {
                var originalDimSet = set.DimensionSet;
                var fourthDimension = new Dimension("4");
                var newDimSet =
                    new DimensionSet(new HashSet<Dimension>
                                     {
                                         originalDimSet.dimensions[0], originalDimSet.dimensions[1],
                                         originalDimSet.dimensions[2], fourthDimension,
                                     });
                var originalKeys = new List<Key>(set.Select(kvp => kvp.Key.Clone() as Key));

                set.Convert(newDimSet);
                Assert.AreSame(newDimSet, set.DimensionSet);

                int currentKey = 0;
                var fourthDimensionIndex = Array.IndexOf(newDimSet.dimensions, fourthDimension);
                foreach (var kvp in set)
                {
                    for (var i = 0; i < newDimSet.dimensions.Length; ++i)
                    {
                        var originalKey = originalKeys[currentKey];

                        if (i == fourthDimensionIndex)
                        {
                            Assert.AreEqual(Key.WildcardDimensionValue, kvp.Key[i]);
                        }

                        else
                        {
                            var newDimension = newDimSet.dimensions[i];
                            var originalDimension =
                                (from d in originalDimSet.dimensions where d.Equals(newDimension) select d).First();
                            var originalDimIndex = Array.IndexOf(originalDimSet.dimensions, originalDimension);
                            Assert.AreEqual(originalDimension.IndexToString(originalKey[originalDimIndex]),
                                            newDimension.IndexToString(kvp.Key[i]));
                        }
                    }

                    ++currentKey;
                }

                Assert.AreEqual(originalKeys.Count, currentKey);

                Assert.AreEqual(0, fourthDimension.Values.Count());
            }
        }
        #endregion
    }
}
