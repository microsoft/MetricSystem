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

    using NUnit.Framework;

    [TestFixture]
    public sealed class KeyTests
    {
        [Test]
        public void EqualityComparisonOfNullKeysProducesExpectedValues()
        {
            Assert.AreEqual(0,  Key.Compare(null, null));
            Assert.AreEqual(-1, Key.Compare(null, new Key(new uint[0])));
            Assert.AreEqual(1, Key.Compare(new Key(new uint[0]), null));
        }

        [Test]
        public void ComparingDifferentLengthKeysThrowsInvalidOperationException()
        {
            try
            {
                Key.Compare(new Key(new uint[0]), new Key(new uint[1]));
                Assert.Fail();
            }
            catch (InvalidOperationException) { }
        }

        [Test]
        public void IdenticalKeysAreEqual()
        {
            Assert.IsTrue(new Key(new uint[0]) == new Key(new uint[0]));
            Assert.IsTrue(new Key(new uint[] { 867 }) == new Key(new uint[] { 867 }));
            Assert.IsTrue(new Key(new uint[] { 867, 5309 }) == new Key(new uint[] { 867, 5309 }));
        }

        [Test]
        public void DifferentKeysAreNotEqual()
        {
            Assert.IsTrue(new Key(new uint[] { 867 }) != new Key(new uint[] { 5309 }));
            Assert.IsTrue(new Key(new uint[] { 867, 5309 }) != new Key(new uint[] { 5309, 5309 }));
        }

        [Test]
        public void KeysWithLowerValuesAreLessThanKeysWithHigherValues()
        {
            Assert.IsTrue(new Key(new uint[] { 867 }) < new Key(new uint[] { 5309 }));
            Assert.IsTrue(new Key(new uint[] { 867, 5309 }) < new Key(new uint[] { 5309, 5309 }));
            Assert.IsTrue(new Key(new uint[] { 5309, 867 }) < new Key(new uint[] { 5309, 5309 }));
        }

        [Test]
        public void KeysWithHigherValuesAreGreaterThanKeysWithLowerValues()
        {
            Assert.IsTrue(new Key(new uint[] { 5309 }) > new Key(new uint[] { 867 }));
            Assert.IsTrue(new Key(new uint[] { 5309, 867 }) > new Key(new uint[] { 867, 5309 }));
            Assert.IsTrue(new Key(new uint[] { 5309, 5309 }) > new Key(new uint[] { 5309, 867 }));
        }

        [Test]
        public void GeneratedWildcardKeysAreValid()
        {
            for (var i = 0; i < 128; ++i)
            {
                var key = Key.GetWildcardKey(DimensionSetTests.CreateDimensionSet(i));
                for (var ki = 0; ki < key.Values.Length; ++ki)
                {
                    Assert.AreEqual(Key.WildcardDimensionValue, key.Values[ki]);
                }
            }
        }
    }
}
