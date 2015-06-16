// ---------------------------------------------------------------------
// <copyright file="KeyTests.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
