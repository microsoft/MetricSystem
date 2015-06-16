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
    using System;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// Represents a source for configuration data.
    /// </summary>
    public abstract class ConfigurationSource : IDisposable
    {
        /// <summary>
        /// Useful as a representation for an empty object when no data is provided.
        /// </summary>
        internal const string EmptyContent = "{}";

        protected bool Disposed { get; private set; }

        public void Dispose()
        {
            this.Disposed = true;
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void CheckDisposed()
        {
            if (this.Disposed)
            {
                throw new ObjectDisposedException(this.GetType().ToString());
            }
        }

        /// <summary>
        /// Read data from the source as a JSON object. Caller is expected to handle any exceptions.
        /// </summary>
        /// <returns>The processed JSON object.</returns>
        internal JObject Read()
        {
            this.CheckDisposed();

            JObject data;
            using (var reader = this.GetReader())
            {
                data = JObject.Load(reader);
            }

            return data;
        }

        /// <summary>
        /// Retrieve a JsonReader used in the <see cref="Read"/> method.
        /// </summary>
        /// <returns>A JsonReader.</returns>
        protected abstract JsonReader GetReader();

        /// <summary>
        /// Triggered when data within the source has been updated.
        /// </summary>
        internal event Action SourceUpdated;

        /// <summary>
        /// Should be called by child classes to indicate that data has been updated.
        /// </summary>
        protected void SetUpdated()
        {
            this.CheckDisposed();

            if (this.SourceUpdated != null)
            {
                this.SourceUpdated();
            }
        }

        protected virtual void Dispose(bool disposing) { }

        ~ConfigurationSource()
        {
            this.Dispose(false);
        }
    }
}
