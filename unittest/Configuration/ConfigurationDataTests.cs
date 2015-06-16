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

namespace MetricSystem.Configuration.UnitTests
{
    using System;
    using System.Collections.Generic;

    using Newtonsoft.Json;

    using NUnit.Framework;

    [TestFixture]
    public sealed class ConfigurationDataTests
    {
        [JsonObject(MemberSerialization = MemberSerialization.OptOut)]
        private sealed class SampleData : ConfigurationBase, IEquatable<SampleData>
        {
            public const int MagicExceptionThrowingNumber = 0xbeef;

            public Dictionary<string, string> Dictionary;
            public List<int> MoreNumbers;
            [JsonConverter(typeof(ExceptionSpewingConverter))]
            public int Number;
            public string String;
            public bool Valid = true;

        private sealed class ExceptionSpewingConverter : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                writer.WriteValue(((int)value).ToString());
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
                                            JsonSerializer serializer)
            {
                int value = int.Parse(reader.Value.ToString());
                if (value == MagicExceptionThrowingNumber)
                {
                    throw new OperationCanceledException();
                }

                return value;
            }

            public override bool CanConvert(Type objectType)
            {
                return objectType == typeof(int);
            }
        }

            public bool Equals(SampleData other)
            {
                if (this.Dictionary != null)
                {
                    if (other.Dictionary == null)
                    {
                        return false;
                    }
                    if (other.Dictionary.Count != this.Dictionary.Count)
                    {
                        return false;
                    }
                    foreach (var kvp in this.Dictionary)
                    {
                        if (!other.Dictionary.ContainsKey(kvp.Key) ||
                            other.Dictionary[kvp.Key] != kvp.Value)
                        {
                            return false;
                        }
                    }
                }
                else if (other.Dictionary != null)
                {
                    return false;
                }

                if (this.MoreNumbers != null)
                {
                    if (other.MoreNumbers == null)
                    {
                        return false;
                    }
                    if (other.MoreNumbers.Count != this.MoreNumbers.Count)
                    {
                        return false;
                    }
                    for (var i = 0; i < this.MoreNumbers.Count; ++i)
                    {
                        if (other.MoreNumbers[i] != this.MoreNumbers[i])
                        {
                            return false;
                        }
                    }
                }
                else if (other.MoreNumbers != null)
                {
                    return false;
                }

                if (other.Number != this.Number)
                {
                    return false;
                }
                if (other.String != this.String)
                {
                    return false;
                }

                return true;
            }

            public override bool Validate()
            {
                return this.Valid;
            }
        }

        [Test]
        public void ArrayDataIsReplacedByLatestSource()
        {
            var baseArray = new List<int> {8, 6, 7};
            var overrideArray = new List<int> {5, 3, 0, 9};
            var baseData = JsonConvert.SerializeObject(new SampleData {MoreNumbers = baseArray});
            var overrideData = JsonConvert.SerializeObject(new SampleData {MoreNumbers = overrideArray});
            using (var firstSource = new TextConfigurationSource(baseData))
            {
                using (var secondSource = new TextConfigurationSource(overrideData))
                {
                    using (var data = new ConfigurationData<SampleData>(new[] {firstSource, secondSource}))
                    {
                        Assert.That(data.Value.MoreNumbers, Is.EquivalentTo(overrideArray));
                    }
                }
            }
        }

        [Test]
        public void CanGetValueFromSingleSource()
        {
            var sampleData = new SampleData
                             {
                                 Dictionary = new Dictionary<string, string> {{"hello", "goodbye"}},
                                 Number = 42,
                                 MoreNumbers = new List<int> {8, 6, 7, 5, 3, 0, 9},
                                 String = string.Empty,
                             };

            using (var source = new TextConfigurationSource(JsonConvert.SerializeObject(sampleData)))
            {
                using (var data = new ConfigurationData<SampleData>(new[] {source}))
                {
                    Assert.IsNotNull(data.Value);
                    Assert.AreEqual(sampleData, data.Value);
                    Assert.IsTrue(data.IsValid);
                }
            }
        }

        [Test]
        public void DictionaryDataIsMergedWithLatestSourceOverridingSameKeys()
        {
            var baseDict = new Dictionary<string, string> {{"one", "direction"}, {"troll", "unittests"}};
            var overrideDict = new Dictionary<string, string> {{"one", "singular sensation"}, {"two", "if by sea"}};
            var baseData = JsonConvert.SerializeObject(new SampleData {Dictionary = baseDict});
            var overrideData = JsonConvert.SerializeObject(new SampleData {Dictionary = overrideDict});
            using (var firstSource = new TextConfigurationSource(baseData))
            {
                using (var secondSource = new TextConfigurationSource(overrideData))
                {
                    using (var data = new ConfigurationData<SampleData>(new[] {firstSource, secondSource}))
                    {
                        Assert.AreEqual(3, data.Value.Dictionary.Count);
                        Assert.AreEqual(baseDict["troll"], data.Value.Dictionary["troll"]);
                        Assert.AreEqual(overrideDict["one"], data.Value.Dictionary["one"]);
                        Assert.AreEqual(overrideDict["two"], data.Value.Dictionary["two"]);
                    }
                }
            }
        }

        [Test]
        public void IfConfigurationIsDeclaredInvalidItIsNotUsed()
        {
            var sampleData = new SampleData {Number = 42, Valid = false};

            using (var source = new TextConfigurationSource(JsonConvert.SerializeObject(sampleData)))
            {
                using (var data = new ConfigurationData<SampleData>(new[] {source}))
                {
                    Assert.IsNull(data.Value);
                    Assert.IsFalse(data.IsValid);
                }
            }
        }

        [Test]
        public void InvalidDataDoesNotOverrideValidData()
        {
            var sampleData = new SampleData {Number = 42,};

            using (var source = new TextConfigurationSource(JsonConvert.SerializeObject(sampleData)))
            {
                using (var data = new ConfigurationData<SampleData>(new[] {source}))
                {
                    Assert.IsNotNull(data.Value);
                    Assert.AreEqual(sampleData, data.Value);

                    var dataValue = data.Value;
                    source.Contents = "not valid";
                    Assert.IsFalse(data.IsValid);
                    Assert.AreSame(dataValue, data.Value);
                }
            }
        }

        [Test]
        public void MayNotProvieNullSources()
        {
            Assert.Throws<ArgumentException>(() => new ConfigurationData<SampleData>(new ConfigurationSource[]
                                                                                     {null}));
        }

        [Test]
        public void MustProvideAtLeastOneValidSource()
        {
            Assert.Throws<ArgumentException>(() => new ConfigurationData<SampleData>(new ConfigurationSource[0]));
            Assert.DoesNotThrow(() => new ConfigurationData<SampleData>(new[] {new TextConfigurationSource()}));
        }

        [Test]
        public void SimplePropertiesAreOverwritten()
        {
            var baseData = JsonConvert.SerializeObject(new SampleData {String = "hello", Number = 867});
            var overrideData = JsonConvert.SerializeObject(new SampleData {String = "goodbye", Number = 5309});
            using (var firstSource = new TextConfigurationSource(baseData))
            {
                using (var secondSource = new TextConfigurationSource(overrideData))
                {
                    using (var data = new ConfigurationData<SampleData>(new[] {firstSource, secondSource}))
                    {
                        Assert.AreEqual("goodbye", data.Value.String);
                        Assert.AreEqual(5309, data.Value.Number);
                    }
                }
            }
        }

        [Test]
        public void UnhandledExceptionsDuringConversionCallExceptionHandler()
        {
            var sampleData = new SampleData
                             {
                                 Dictionary = new Dictionary<string, string> {{"hello", "goodbye"}},
                                 Number = 42,
                                 MoreNumbers = new List<int> {8, 6, 7, 5, 3, 0, 9},
                                 String = string.Empty,
                             };

            using (var source = new TextConfigurationSource(JsonConvert.SerializeObject(sampleData)))
            {
                using (var data = new ConfigurationData<SampleData>(new[] {source}))
                {
                    Assert.IsNotNull(data.Value);
                    Assert.AreEqual(sampleData, data.Value);
                    Assert.IsTrue(data.IsValid);

                    var exceptionEventCalled = false;
                    data.UnhandledException += (src, ex) => exceptionEventCalled = true;

                    sampleData.Number = SampleData.MagicExceptionThrowingNumber;
                    Assert.Throws<OperationCanceledException>(
                                                              () =>
                                                              source.Contents = JsonConvert.SerializeObject(sampleData));
                    Assert.IsTrue(exceptionEventCalled);
                }
            }
        }
    }
}
