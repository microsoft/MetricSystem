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
