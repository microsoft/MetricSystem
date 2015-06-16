// ---------------------------------------------------------------------
// <copyright file="DataKeyTests.cs" company="Microsoft">
//       Copyright 2013 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Data.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;

    using MetricSystem.Utilities;

    using Newtonsoft.Json;

    using NUnit.Framework;

    [TestFixture]
    public sealed class DimensionSetTests
    {
        public static DimensionSet CreateDimensionSet(int numberOfDimensions)
        {
            var set = new HashSet<Dimension>();
            for (var d = 0; d < numberOfDimensions; ++d)
            {
                set.Add(new Dimension(d.ToString(CultureInfo.InvariantCulture)));
            }

            return new DimensionSet(set);
        }

        [Test]
        public void CanConstructDimensionSet()
        {
            new DimensionSet(CreateDimensionSet(3));
        }

        [Test]
        public void CanConstructDimensionSetWithNoDimensions()
        {
            new DimensionSet(CreateDimensionSet(0));
        }

        [Test]
        public void ConstructorThrowsArgumentNullExceptionForNullDimensionCollection()
        {
            Assert.Throws<ArgumentNullException>(() => new DimensionSet((ISet<Dimension>)null));
        }

        [Test]
        public void ConstructorThrowsArgumentExceptionIfDimensionIsInReservedList()
        {
            Assert.Throws<ArgumentException>(() =>
                                             new DimensionSet(new HashSet<Dimension>
                                                              {
                                                                  new Dimension(
                                                                      Dimension.ReservedDimensionNames.First())
                                                              }));
        }

        [Test]
        public void ConstructorThrowsArgumentExceptionIfDimensionIsNullEmptyOrWhitespace()
        {
            Assert.Throws<ArgumentException>(() => new DimensionSet(new HashSet<Dimension> {null}));
            Assert.Throws<ArgumentException>(() => new DimensionSet(new HashSet<Dimension> {new Dimension(string.Empty)}));
            Assert.Throws<ArgumentException>(() => new DimensionSet(new HashSet<Dimension> {new Dimension(" ")}));
        }

        [Test]
        public void AllSpecialDimensionsAreInReservedList()
        {
            Assert.IsTrue(Dimension.ReservedDimensionNames.Contains(ReservedDimensions.AggregateSamplesDimension));
            Assert.IsTrue(Dimension.ReservedDimensionNames.Contains(ReservedDimensions.MachineDimension));
            Assert.IsTrue(Dimension.ReservedDimensionNames.Contains(ReservedDimensions.EnvironmentDimension));
            Assert.IsTrue(Dimension.ReservedDimensionNames.Contains(ReservedDimensions.DatacenterDimension));
            Assert.IsTrue(Dimension.ReservedDimensionNames.Contains(ReservedDimensions.StartTimeDimension));
            Assert.IsTrue(Dimension.ReservedDimensionNames.Contains(ReservedDimensions.EndTimeDimension));
            Assert.IsTrue(Dimension.ReservedDimensionNames.Contains(ReservedDimensions.PercentileDimension));
            Assert.IsTrue(Dimension.ReservedDimensionNames.Contains(ReservedDimensions.DimensionDimension));
        }

        [Test]
        public void CreateKeyIgnoresExtraDimensions()
        {
            const string anyDim1 = "d1";
            const string anyDim2 = "d2";
            var dimensionSet = new DimensionSet(new HashSet<Dimension> {new Dimension(anyDim1), new Dimension(anyDim2)});
            var anyDimensionValues = new DimensionSpecification {{anyDim1, "anyVal1"}, {anyDim2, "anyVal2"}};

            bool allDimensionsProvided;
            Key key1 = dimensionSet.CreateKey(anyDimensionValues, out allDimensionsProvided);
            Key key2 = dimensionSet.CreateKey(new DimensionSpecification(anyDimensionValues) {{"newDim", "anyVal"}},
                out allDimensionsProvided);
            Assert.AreEqual(key1, key2);
        }

        [Test]
        public void CreateKeySetsAllDimensionsProvidedTrueIfAllDimensionsAreInDictionary()
        {
            const string anyDim1 = "d1";
            const string anyDim2 = "d2";
            var dimensionSet = new DimensionSet(new HashSet<Dimension> {new Dimension(anyDim1), new Dimension(anyDim2)});
            var allDimensionValues = new DimensionSpecification {{anyDim1, "anyVal1"}, {anyDim2, "anyVal2"}};

            bool allDimensionsProvided;
            dimensionSet.CreateKey(allDimensionValues, out allDimensionsProvided);
            Assert.AreEqual(true, allDimensionsProvided);
        }

        [Test]
        public void CreateKeySetsAllDimensionsProvidedFalseIfSomeDimensionsAreNotProvided()
        {
            const string anyDim1 = "d1";
            const string anyDim2 = "d2";
            var dimensionSet = new DimensionSet(new HashSet<Dimension> {new Dimension(anyDim1), new Dimension(anyDim2)});
            var partialDimensionValues = new DimensionSpecification {{anyDim1, "anyVal1"}};

            bool allDimensionsProvided;
            dimensionSet.CreateKey(partialDimensionValues, out allDimensionsProvided);
            Assert.AreEqual(false, allDimensionsProvided);
        }

        [Test]
        public void MatchIsTrueIfKeyAndFilterAreSame()
        {
            const string anyDim1 = "d1";
            const string anyDim2 = "d2";
            const string anyDim3 = "d3";
            var dimensionSet = new DimensionSet(new HashSet<Dimension>
                                           {
                                               new Dimension(anyDim1),
                                               new Dimension(anyDim2),
                                               new Dimension(anyDim3),
                                           });
            var keyValueSet = new DimensionSpecification {{anyDim1, "val1"}, {anyDim2, "val2"}, {anyDim3, "val3"}};
            bool allDimensionsProvided;
            Key key = dimensionSet.CreateKey(keyValueSet, out allDimensionsProvided);

            Assert.IsTrue(key.Matches(key));
        }

        [Test]
        public void MatchIsTrueIfNoValuesAreProvidedInFilter()
        {
            const string anyDim1 = "d1";
            const string anyDim2 = "d2";
            const string anyDim3 = "d3";
            var dimensionSet = new DimensionSet(new HashSet<Dimension>
                                           {
                                               new Dimension(anyDim1),
                                               new Dimension(anyDim2),
                                               new Dimension(anyDim3),
                                           });
            var keyValueSet = new DimensionSpecification {{anyDim1, "val1"}, {anyDim2, "val2"}, {anyDim3, "val3"}};
            bool allDimensionsProvided;
            Key key = dimensionSet.CreateKey(keyValueSet, out allDimensionsProvided);
            Key filter = dimensionSet.CreateKey(new DimensionSpecification(), out allDimensionsProvided);

            Assert.IsTrue(filter.Matches(key));
        }

        [Test]
        public void MatchIsTrueIfSomeSameValuesAreProvidedInFilter()
        {
            const string anyDim1 = "d1";
            const string anyDim2 = "d2";
            const string anyDim3 = "d3";
            var dimensionSet = new DimensionSet(new HashSet<Dimension>
                                           {
                                               new Dimension(anyDim1),
                                               new Dimension(anyDim2),
                                               new Dimension(anyDim3),
                                           });
            var keyValueSet = new DimensionSpecification {{anyDim1, "val1"}, {anyDim2, "val2"}, {anyDim3, "val3"}};
            bool allDimensionsProvided;
            Key key = dimensionSet.CreateKey(keyValueSet, out allDimensionsProvided);
            Key filter = dimensionSet.CreateKey(new DimensionSpecification {{anyDim2, "val2"}}, out allDimensionsProvided);

            Assert.IsTrue(filter.Matches(key));
        }

        [Test]
        public void MatchIsFalseIfFilterHasMoreOrLessValuesThanKey()
        {
            const string anyDim1 = "d1";
            const string anyDim2 = "d2";
            const string anyDim3 = "d3";
            var dimensionSet = new DimensionSet(new HashSet<Dimension>
                                           {
                                               new Dimension(anyDim1),
                                               new Dimension(anyDim2),
                                               new Dimension(anyDim3),
                                           });
            var keyValueSet = new DimensionSpecification {{anyDim1, "val1"}, {anyDim2, "val2"}, {anyDim3, "val3"}};
            bool allDimensionsProvided;
            Key key = dimensionSet.CreateKey(keyValueSet, out allDimensionsProvided);

            var smallerDimensionSet =
                new DimensionSet(new HashSet<Dimension> {new Dimension(anyDim1), new Dimension(anyDim2)});
            Key smallerFilter = smallerDimensionSet.CreateKey(keyValueSet, out allDimensionsProvided);
            Assert.IsFalse(smallerFilter.Matches(key));

            var largerDimensionSet = new DimensionSet(new HashSet<Dimension>(dimensionSet.Dimensions)
                                                      {
                                                          new Dimension
                                                              ("anotherDim")
                                                      });
            Key largerFilter = largerDimensionSet.CreateKey(keyValueSet, out allDimensionsProvided);
            Assert.IsFalse(largerFilter.Matches(key));
        }

        [Test]
        public void MatchIsFalseIfValuesDiffer()
        {
            const string anyDim1 = "d1";
            const string anyDim2 = "d2";
            const string anyDim3 = "d3";
            var dimensionSet = new DimensionSet(new HashSet<Dimension>
                                           {
                                               new Dimension(anyDim1),
                                               new Dimension(anyDim2),
                                               new Dimension(anyDim3),
                                           });
            var keyValueSet = new DimensionSpecification {{anyDim1, "val1"}, {anyDim2, "val2"}, {anyDim3, "val3"}};
            bool allDimensionsProvided;
            Key key = dimensionSet.CreateKey(keyValueSet, out allDimensionsProvided);

            // Longer values
            foreach (var d in dimensionSet.Dimensions)
            {
                var filterValueSet = new DimensionSpecification(keyValueSet);
                filterValueSet[d.Name] = "valAnyOther";
                Key filter = dimensionSet.CreateKey(filterValueSet, out allDimensionsProvided);
                Assert.IsFalse(filter.Matches(key));
            }

            // Same length
            foreach (var d in dimensionSet.Dimensions)
            {
                var filterValueSet = new DimensionSpecification(keyValueSet);
                filterValueSet[d.Name] = "valX";
                Key filter = dimensionSet.CreateKey(filterValueSet, out allDimensionsProvided);
                Assert.IsFalse(filter.Matches(key));
            }

            // Shorter length
            foreach (var d in dimensionSet.Dimensions)
            {
                var filterValueSet = new DimensionSpecification(keyValueSet);
                filterValueSet[d.Name] = "v";
                Key filter = dimensionSet.CreateKey(filterValueSet, out allDimensionsProvided);
                Assert.IsFalse(filter.Matches(key));
            }
        }

        [Test]
        public void ConstructorHasDimensionsOrderedByDistinctValueCount()
        {
            var bigDimension = new Dimension("big");
            bigDimension.StringToIndex("1");
            bigDimension.StringToIndex("2");
            bigDimension.StringToIndex("3");

            var mediumDimension = new Dimension("medium");
            mediumDimension.StringToIndex("1");
            mediumDimension.StringToIndex("2");

            var smallDimension = new Dimension("small");
            smallDimension.StringToIndex("1");

            var set = new DimensionSet(new HashSet<Dimension> {smallDimension, mediumDimension, bigDimension});

            Assert.AreSame(set.dimensions[0], bigDimension);
            Assert.AreSame(set.dimensions[1], mediumDimension);
            Assert.AreSame(set.dimensions[2], smallDimension);

            // Now make 'small' actually the largest.
            smallDimension.StringToIndex("2");
            smallDimension.StringToIndex("3");
            smallDimension.StringToIndex("4");

            var newSet = new DimensionSet(set);
            Assert.AreEqual(newSet.dimensions[0], smallDimension);
            Assert.AreEqual(newSet.dimensions[1], bigDimension);
            Assert.AreEqual(newSet.dimensions[2], mediumDimension);
        }

        [Test]
        public void CanSerializeAndDeserializeDimensionSets()
        {
            var set = new DimensionSet(new HashSet<Dimension>
                                       {
                                           new Dimension("one"),
                                           new Dimension("two", new HashSet<string> {"2", "ii"}),
                                           new Dimension("three"),
                                       });

            string json;
            var serializer = new JsonSerializer();
            using (var writer = new StringWriter())
            {
                serializer.Serialize(writer, set);
                json = writer.ToString();
            }

            using (var reader = new StringReader(json))
            using (var jsonReader = new JsonTextReader(reader))
            {
                var readSet = serializer.Deserialize<DimensionSet>(jsonReader);
                Assert.IsTrue(readSet.Equals(set));
                foreach (var dim in set.Dimensions)
                {
                    var readDim = readSet.Dimensions.First(d => d.Name.Equals(dim.Name));
                    if (dim.AllowedValues != null)
                    {
                        Assert.AreEqual(dim.AllowedValues.Count, readDim.AllowedValues.Count);
                        foreach (var v in dim.AllowedValues)
                        {
                            Assert.IsTrue(readDim.AllowedValues.Contains(v));
                        }
                    }
                    else
                    {
                        Assert.IsNull(readDim.AllowedValues);
                    }
                }
            }
        }

        [Test]
        public unsafe void TruncatedWrittenDataThrowsPersistedDataException()
        {
            const int maxDims = 5;
            const int maxValuesPerDim = 5;

            var dimHashSet = new HashSet<Dimension>();
            for (var i = 0; i < maxDims; ++i)
            {
                dimHashSet.Clear();
                for (var di = 0; di < i; ++di)
                {
                    dimHashSet.Add(new Dimension(di.ToString()));
                }

                for (var v = 0; v < maxValuesPerDim; ++v)
                {
                    for (var dv = 0; dv < v; ++dv)
                    {
                        foreach (var dim in dimHashSet)
                        {
                            dim.StringToIndex(dv.ToString());
                        }
                    }

                    byte[] data;
                    var dimSet = new DimensionSet(dimHashSet);
                    using (var ms = new MemoryStream())
                    {
                        dimSet.Write(new BufferWriter(ms));
                        data = ms.GetBuffer();
                        var dataLength = ms.Length;

                        fixed (byte* buffer = data)
                        {
                            var readDS = new DimensionSet(new BufferReader(buffer, dataLength));
                            Assert.IsTrue(dimSet.Equals(readDS));
                            foreach (var dim in dimSet.dimensions)
                            {
                                var readDim = readDS.Dimensions.First(d => d.Name.Equals(dim.Name));
                                foreach (var val in dim.Values)
                                {
                                    Assert.IsTrue(readDim.Values.Contains(val));
                                }
                            }

                            for (var badLength = dataLength - 1; badLength > 0; --badLength)
                            {
                                try
                                {
                                    new DimensionSet(new BufferReader(buffer, badLength));
                                    Assert.Fail(); // Can't use assert.throws because of the pointer usage here.
                                }
                                catch (PersistedDataException) { }
                            }
                        }
                    }
                }
            }
        }
    }
}
