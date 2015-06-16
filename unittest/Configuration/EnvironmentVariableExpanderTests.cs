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
    using System.IO;

    using Newtonsoft.Json;

    using NUnit.Framework;

    [TestFixture]
    public sealed class EnvironmentVariableExpanderTests
    {
        private const string Variable = "EXPANDER_TEST_VARIAbLE";
        private const string Value = "Variable value";
        private const string JsonText = @"{""Expanded"":""%" + Variable + @"%"",""Unexpanded"":""%" + Variable + @"%""}";

        private readonly JsonSerializer serializer = new JsonSerializer();

        [JsonObject(MemberSerialization = MemberSerialization.OptOut)]
        private sealed class ExpandableData
        {
            [JsonConverter(typeof(EnvironmentVariableExpander))]
            public string Expanded = string.Empty;

            public string Unexpanded = string.Empty;
        }

        [Test]
        public void EnvironmentVariableIsExpandedByConverter()
        {
            Environment.SetEnvironmentVariable(Variable, Value);

            using (var textReader = new StringReader(JsonText))
            {
                using (var jsonReader = new JsonTextReader(textReader))
                {
                    var data = this.serializer.Deserialize<ExpandableData>(jsonReader);

                    Assert.AreEqual(Value, data.Expanded);
                    Assert.AreEqual("%" + Variable + "%", data.Unexpanded);
                }
            }

            Environment.SetEnvironmentVariable(Variable, null);
        }
    }
}
