// ---------------------------------------------------------------------
// <copyright file="EnvironmentVariableExpanderTests.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
