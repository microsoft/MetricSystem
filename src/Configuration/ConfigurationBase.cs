// ---------------------------------------------------------------------
// <copyright file="ConfigurationBase.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Configuration
{
    using System.Collections.Generic;

    using Newtonsoft.Json;

    [JsonObject]
    public abstract class ConfigurationBase
    {
        // Used only in logging, the name is intentional to make collision extremely unlikely.
        [JsonIgnore]
        internal ICollection<ConfigurationSource> __Sources__ { private get; set; }

        /// <summary>
        /// Validate the configuration object.
        /// </summary>
        /// <returns>True if the configuration object is in a valid state.</returns>
        public abstract bool Validate();

        /// <summary>
        /// Write an informational log message.
        /// </summary>
        /// <param name="format">Message format.</param>
        /// <param name="args">Formatted parameters.</param>
        protected void LogInfo(string format, params object[] args)
        {
            this.LogInfo(string.Format(format, args));
        }

        /// <summary>
        /// Write an informational log message.
        /// </summary>
        /// <param name="message">Message text.</param>
        protected void LogInfo(string message)
        {
            Events.Write.Info(this.__Sources__, message);
        }

        /// <summary>
        /// Write a warning log message.
        /// </summary>
        /// <param name="format">Message format.</param>
        /// <param name="args">Formatted parameters.</param>
        protected void LogWarning(string format, params object[] args)
        {
            this.LogWarning(string.Format(format, args));
        }

        /// <summary>
        /// Write a warning log message.
        /// </summary>
        /// <param name="message">Message text.</param>
        protected void LogWarning(string message)
        {
            Events.Write.Warning(this.__Sources__, message);
        }

        /// <summary>
        /// Write an error log message.
        /// </summary>
        /// <param name="format">Message format.</param>
        /// <param name="args">Formatted parameters.</param>
        protected void LogError(string format, params object[] args)
        {
            this.LogError(string.Format(format, args));
        }

        /// <summary>
        /// Write an error log message.
        /// </summary>
        /// <param name="message">Message text.</param>
        protected void LogError(string message)
        {
            Events.Write.Error(this.__Sources__, message);
        }
    }
}
