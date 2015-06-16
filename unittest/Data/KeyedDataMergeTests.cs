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

    using NUnit.Framework;

    [TestFixture]
    public sealed class KeyedDataMergeTests
    {
        private struct LongData : IMergeSource, IMergeableData
        {
            public bool MultiValue { get { return false; } }
            public long Value;

            public void Clear()
            {
                this.Value = 0;
            }

            public void MergeFrom(IMergeSource other)
            {
                this.Value += ((LongData)other).Value;
            }

            public LongData(long value)
            {
                this.Value = value;
            }
        }

        [Test]
        public void CanMergeEmptyData()
        {
            Assert.IsFalse(KeyedDataMerge<LongData>.MergeSorted(
                                                            new[]
                                                            {
                                                                new List<KeyValuePair<Key, IMergeSource>>(),
                                                                new List<KeyValuePair<Key, IMergeSource>>(),
                                                            }).Any());
        }

        [Test]
        public void CanMergeZeroDepthData()
        {
            var left = new LongData(867);
            var right = new LongData(5309);
            var leftData = new List<KeyValuePair<Key, IMergeSource>>
                           {
                               new KeyValuePair<Key, IMergeSource>(new Key(new uint[0]), left),
                           };

            var rightData = new List<KeyValuePair<Key, IMergeSource>>
                            {
                                new KeyValuePair<Key, IMergeSource>(new Key(new uint[0]), right),
                            };

            var merged = KeyedDataMerge<LongData>.MergeSorted(new[] {leftData, rightData}).ToList();
            Assert.AreEqual(1, merged.Count);
            Assert.AreEqual(left.Value + right.Value, merged[0].Value.Value);
        }

        [Test]
        public void CanMergeDataWhereOneSideIsEmpty()
        {
            var firstKey = new LongData(867);
            var secondKey = new LongData(5309);
            var fullData = new List<KeyValuePair<Key, IMergeSource>>
                           {
                               new KeyValuePair<Key, IMergeSource>(new Key(new[] {(uint)firstKey.Value}), firstKey),
                               new KeyValuePair<Key, IMergeSource>(new Key(new[] {(uint)secondKey.Value}), secondKey),
                           };

            var emptyData = new List<KeyValuePair<Key, IMergeSource>>();

            // Validate both left and right being the full side.
            foreach (var pair in
                new[]
                {
                    new[] {fullData, emptyData},
                    new[] {emptyData, fullData},
                })
            {
                var count = 0;
                foreach (var kvp in KeyedDataMerge<LongData>.MergeSorted(pair))
                {
                    ++count;
                    if (kvp.Key.Values[0] == (uint)firstKey.Value)
                    {
                        Assert.AreEqual(firstKey.Value, kvp.Value.Value);
                    }
                    else if (kvp.Key.Values[0] == (uint)secondKey.Value)
                    {
                        Assert.AreEqual(secondKey.Value, kvp.Value.Value);
                    }
                    else
                    {
                        Assert.Fail();
                    }
                }

                Assert.AreEqual(2, count);
            }
        }

        [Test]
        public void MergeTreesCombinesData()
        {
            const int possibleValuesByDimension = 100;

            var leftData = new List<KeyValuePair<Key, IMergeSource>>();
            var rightData = new List<KeyValuePair<Key, IMergeSource>>();

            var insertKey = new Key(new uint[3]);

            // Create a reasonably complex pair of trees which overlap when all keys are divisible by 6, and are
            // somewhat sparse.
            var expectedCount = 0;
            var expectedOverlap = 0;
            for (uint d1 = 0; d1 < possibleValuesByDimension; ++d1)
            {
                insertKey.Values[0] = d1;
                var left1 = d1 % 2 == 0;
                var right1 = d1 % 3 == 0;

                for (uint d2 = 0; d2 < possibleValuesByDimension; ++d2)
                {
                    insertKey.Values[1] = d2;
                    var left2 = d2 % 2 == 0;
                    var right2 = d2 % 3 == 0;

                    for (uint d3 = 0; d3 < possibleValuesByDimension; ++d3)
                    {
                        insertKey.Values[2] = d3;
                        var left3 = d3 % 2 == 0;
                        var right3 = d3 % 3 == 0;

                        var value = (d1 + d2 + d3);
                        var wroteValue = false;
                        if (left1 && left2 && left3)
                        {
                            leftData.Add(new KeyValuePair<Key, IMergeSource>(new Key(insertKey), new LongData((int)value)));
                            wroteValue = true;
                        }
                        if (right1 && right2 && right3)
                        {
                            rightData.Add(new KeyValuePair<Key, IMergeSource>(new Key(insertKey), new LongData((int)value)));
                            expectedOverlap += (wroteValue ? 1 : 0);
                            wroteValue = true;
                        }

                        expectedCount += (wroteValue ? 1 : 0);
                    }
                }
            }

            var count = 0;
            var overlapCount = 0;
            foreach (var kvp in KeyedDataMerge<LongData>.MergeSorted(new[] {leftData, rightData}))
            {
                var key = kvp.Key;
                var value = kvp.Value;
                ++count;

                var expectedValues = new List<uint>();
                var match = false;
                if (key.Values[0] % 2 == 0 && key.Values[1] % 2 == 0 && key.Values[2] % 2 == 0)
                {
                    expectedValues.Add(key.Values[0] + key.Values[1] + key.Values[2]);
                    match = true;
                }
                if (key.Values[0] % 3 == 0 && key.Values[1] % 3 == 0 && key.Values[2] % 3 == 0)
                {
                    if (match)
                    {
                        ++overlapCount;
                    }
                    expectedValues.Add(key.Values[0] + key.Values[1] + key.Values[2]);
                }

                Assert.AreEqual(expectedValues.Sum(x => x), value.Value);
            }

            Assert.AreEqual(expectedCount, count);
            Assert.AreEqual(expectedOverlap, overlapCount);
        }

        [Test]
        public unsafe void CanMergeManySources()
        {
            const int writeCount = 100000;
            const int maxKeyValue = 20; // we want a lot of collisions.

            var data = new BufferedKeyedData<int>[20];
            var dimensionSet = DimensionSetTests.CreateDimensionSet(1);
            for (var i = 0; i < data.Length; ++i)
            {
                data[i] =
                    new BufferedKeyedData<int>(
                        new byte[
                            BufferedKeyedData<int>.GetBufferSizeForKeyCount(writeCount / data.Length, dimensionSet)], 0,
                        0, new DimensionSet(dimensionSet), true);
            }

            var rng = new Random();
            var expected = new Dictionary<uint, List<int>>();

            for (int i = 0; i < writeCount; ++i)
            {
                var key = (uint)rng.Next(maxKeyValue);
                Assert.IsTrue(data[i % data.Length].TryWrite(&key, i));

                List<int> expectedValuesForKey;
                if (!expected.TryGetValue(key, out expectedValuesForKey))
                {
                    expectedValuesForKey = new List<int>();
                    expected.Add(key, expectedValuesForKey);
                }
                expectedValuesForKey.Add(i);
            }

            var dataToMerge = new List<IEnumerable<KeyValuePair<Key, IMergeSource>>>(data.Length);
            foreach (var d in data)
            {
                d.Seal();
                d.Sort();

                var chunk = new List<KeyValuePair<Key, IMergeSource>>(d.Count);
                chunk.AddRange(d.Select(kvp => new KeyValuePair<Key, IMergeSource>(kvp.Key.Clone() as Key, new LongData(kvp.Value))));
                dataToMerge.Add(chunk);
            }

            Key currentKey = null;
            foreach (var kvp in KeyedDataMerge<LongData>.MergeSorted(dataToMerge))
            {
                Assert.IsTrue(currentKey < kvp.Key);
                currentKey = kvp.Key.Clone() as Key;

                var expectedData = expected[kvp.Key[0]];
                Assert.AreEqual(expectedData.Sum(), kvp.Value.Value);
                expected.Remove(kvp.Key[0]);
            }
        }
    }
}
