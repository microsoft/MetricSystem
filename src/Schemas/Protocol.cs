// ---------------------------------------------------------------------
// <copyright file="Protocol.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;

    public static class Protocol
    {
        public const string TimestampStringFormat = "o"; // ISO 8601 format.
        public const string BondCompactBinaryMimeType = "bond/compact-binary";
        public const string ApplicationJsonMimeType = "application/json";
        public const ushort DefaultServerPort = 4200;

        /// <summary>
        /// Validates the name of a counter.
        /// </summary>
        /// <param name="counterName">Counter name to validate.</param>
        /// <returns>True if the counter name is valid and usable</returns>
        /// <remarks>
        /// Counter names must obey the following rules:
        /// - They must begin with a /.
        /// - They must NOT end with a /.
        /// - They must NOT be a single / with no additional content.
        /// - They must NOT contain \ characters.
        /// - They must NOT consist solely of whitespace.
        /// - They must NOT end with whitespace.
        /// - They must be contain only valid path characters for the host system.
        /// </remarks>
        public static bool IsValidCounterName(string counterName)
        {
            return !string.IsNullOrEmpty(counterName) && counterName.StartsWith("/") && !counterName.EndsWith("/") &&
                   !counterName.Contains("\\") && counterName.IndexOfAny(Path.GetInvalidPathChars()) < 0 &&
                   !char.IsWhiteSpace(counterName[counterName.Length - 1]);
        }

        /// <summary>
        /// Build the path and parameters portion of a request.
        /// </summary>
        /// <param name="operation">Operation to perform (e.g. /counters)</param>
        /// <param name="counterName">Counter name or pattern to apply.</param>
        /// <param name="queryParameters">Optional query parameters for the request.</param>
        /// <returns>A string representing the built command.</returns>
        public static string BuildCounterRequestCommand(string operation, string counterName,
                                                        IDictionary<string, string> queryParameters)
        {
            if (string.IsNullOrEmpty(operation))
            {
                throw new ArgumentException("Invalid operation", "operation");
            }

            var builder = new StringBuilder();
            if (string.IsNullOrEmpty(counterName))
            {
                counterName = "/*"; // hmmm
            }
            else if (!IsValidCounterName(counterName))
            {
                throw new ArgumentException("Counter name is invalid.");
            }

            builder.AppendFormat("{0}{1}/{2}", RestCommands.CounterRequestCommand, counterName, operation);
            if (queryParameters != null && queryParameters.Count > 0)
            {
                builder.Append('?');
                builder.Append(string.Join("&",
                                           from pair in queryParameters
                                           select
                                               Uri.EscapeDataString(pair.Key) + '=' + Uri.EscapeDataString(pair.Value)));
            }

            return builder.ToString();
        }
    }
}
