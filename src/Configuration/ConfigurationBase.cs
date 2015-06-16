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
