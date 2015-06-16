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

    using Microsoft.IO;

    using NUnit.Framework;

    using TestKeyedDataStore = KeyedDataStore<InternalHistogram>;

    [TestFixture]
    public sealed class KeyedDataStoreTests
    {
        private const int MaxDimensionValue = 200;

        private TestKeyedDataStore dataStore;
        private DimensionSet dimensionSet;
        private RecyclableMemoryStreamManager memoryStreamManager;
        [ThreadStatic]
        private static Random rng;

        [SetUp]
        public void SetUp()
        {
            if (this.memoryStreamManager == null)
            {
                this.memoryStreamManager = new RecyclableMemoryStreamManager(1 << 17, 1 << 20, 1 << 24);
            }

            this.dimensionSet = DimensionSetTests.CreateDimensionSet(2);
            this.dataStore = new TestKeyedDataStore(this.dimensionSet, this.memoryStreamManager);
        }

        [TearDown]
        public void TearDown()
        {
            this.dataStore.Dispose();
        }

        [Test]
        public void CanReadSerializedData()
        {
            const int values = 5309;
            this.WriteRandomTestData(values, this.dataStore);
            this.dataStore.Merge();
            var uniqueKeys = this.dataStore.Count;

            var dataStream = new MemoryStream();
            this.dataStore.Serialize(dataStream);

            this.dataStore.Dispose();
            this.dataStore = new TestKeyedDataStore(new DimensionSet(this.dimensionSet), this.memoryStreamManager,
                                                    dataStream, uniqueKeys, PersistedDataType.VariableEncodedHistogram);
            this.EnsureRandomTestData(values, this.dataStore);
        }

        [Test]
        public void CanReadIndividualWrittenDataOnlyAfterMerging()
        {
            const int values = 30000;
            this.WriteRandomTestData(values, this.dataStore);

            Assert.AreEqual(0, this.dataStore.Count);
            Assert.IsFalse(this.dataStore.Any());

            this.dataStore.Merge();
            Assert.AreNotEqual(0, this.dataStore.Count);
            this.EnsureRandomTestData(values, this.dataStore);
        }

        [Test]
        public void DataStoresAreThreadSafe()
        {
            Parallel.For(0, 200,
                         i =>
                         {
                             using (
                                 var store = new TestKeyedDataStore(new DimensionSet(this.dimensionSet),
                                                                    this.memoryStreamManager))
                             {
                                 var valueCount = GetRandom(100000);
                                 this.WriteRandomTestData(valueCount, store);
                                 store.Merge();
                                 this.EnsureRandomTestData(valueCount, store);
                             }
                         });
        }

        private void EnsureRandomTestData(int maxValue, TestKeyedDataStore store)
        {
            var expected = new HashSet<long>();
            for (int i = 0; i < maxValue; ++i)
            {
                expected.Add(i);
            }

            foreach (var kvp in store)
            {
                foreach (var val in kvp.Value.Data.Keys)
                {
                    Assert.IsTrue(expected.Remove(val));
                }
            }

            Assert.AreEqual(0, expected.Count);
        }

        private static int GetRandom(int maxValue)
        {
            if (rng == null)
            {
                rng = new Random();
            }

            return rng.Next(maxValue);
        }

        private void GenerateRandomKey(DimensionSpecification dims)
        {
            dims.Clear();
            foreach (var d in this.dimensionSet.Dimensions)
            {
                dims.Add(d.Name, GetRandom(MaxDimensionValue).ToString());
            }
        }

        private void WriteRandomTestData(int maxValue, TestKeyedDataStore store)
        {
            Parallel.For(0, maxValue,
                         i =>
                         {
                             var dims = new DimensionSpecification();
                             this.GenerateRandomKey(dims);
                             store.AddValue(dims, i);
                         });
        }
    }
}
