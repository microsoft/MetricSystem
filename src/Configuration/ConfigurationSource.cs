// ---------------------------------------------------------------------
// <copyright file="ConfigurationSource.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
