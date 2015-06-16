// ---------------------------------------------------------------------
// <copyright file="EnvironmentVariableExpander.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Configuration
{
    using System;

    using Newtonsoft.Json;

    /// <summary>
    /// Property converter which handles expanding environment variables contained within a string.
    /// Re-serialized values which were previously expanded will contain their expanded, not original, text.
    /// </summary>
    public sealed class EnvironmentVariableExpander : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(value as string);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue,
                                        JsonSerializer serializer)
        {
            return Environment.ExpandEnvironmentVariables(reader.Value.ToString());
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(string);
        }
    }
}
