// ---------------------------------------------------------------------
// <copyright file="FileConfigurationSource.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Configuration
{
    using System;
    using System.IO;

    using Newtonsoft.Json;

    /// <summary>
    /// A configuration source backed by file data. Handles updates when a file appears or is modified.
    /// </summary>
    public sealed class FileConfigurationSource : ConfigurationSource
    {
        private readonly string fullFilename;

        private DateTime currentLastWrite;
        private FileSystemWatcher fileWatcher;

        /// <summary>
        /// Construct a configuration source with data backed by the given filename.
        /// </summary>
        /// <param name="filename">The file to watch (path may be relative or absolute).</param>
        public FileConfigurationSource(string filename)
        {
            this.fullFilename = Path.GetFullPath(filename);
            var directory = Path.GetDirectoryName(this.fullFilename);
            filename = Path.GetFileName(this.fullFilename);

            this.fileWatcher = new FileSystemWatcher(directory, filename);
            this.fileWatcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size;
            this.fileWatcher.Created += this.OnChanged;
            this.fileWatcher.Changed += this.OnChanged;
            this.fileWatcher.EnableRaisingEvents = true;

            this.OnChanged(null, null);
        }

        private void OnChanged(object sender, FileSystemEventArgs args)
        {
            this.CheckDisposed();
            lock (this)
            {
                var lastWrite = File.GetLastWriteTime(this.fullFilename);
                if (this.currentLastWrite != lastWrite)
                {
                    this.currentLastWrite = lastWrite;
                    this.SetUpdated();
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (this.fileWatcher != null)
            {
                this.fileWatcher.Dispose();
                this.fileWatcher = null;
            }
        }

        protected override JsonReader GetReader()
        {
            if (File.Exists(this.fullFilename))
            {
                return
                    new JsonTextReader(
                        new StreamReader(new FileStream(this.fullFilename, FileMode.Open, FileAccess.Read)));
            }

            return new JsonTextReader(new StringReader(EmptyContent));
        }

        public override string ToString()
        {
            return this.fullFilename;
        }
    }
}
