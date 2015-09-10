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

namespace MetricSystem.Data
{
    using MetricSystem.Utilities;

    /// <summary>
    /// Possible statuses for a given data source.
    /// </summary>
    internal enum PersistedDataSourceStatus
    {
        /// <summary>
        /// The source's status is not currently known.
        /// </summary>
        Unknown,

        /// <summary>
        /// The source data is available.
        /// </summary>
        Available,

        /// <summary>
        /// The source data is unavailable.
        /// </summary>
        Unavailable,

        /// <summary>
        /// The source data is partially available (mix of Available and
        /// Unknown)
        /// </summary>
        Partial
    }

    /// <summary>
    /// Describes a potential source of persisted data.
    /// </summary>
    internal sealed class PersistedDataSource
    {
        public PersistedDataSource(string name, PersistedDataSourceStatus status)
        {
            this.Name = name;
            this.Status = status;
        }

        public PersistedDataSource(BufferReader reader)
        {
            var start = reader.BytesRead;
            this.Name = reader.ReadString();
            this.Status = (PersistedDataSourceStatus)reader.ReadVariableLengthInt32();
            this.SerializedSize = reader.BytesRead - start;
        }

        /// <summary>
        /// Name (typically machine name) of the source.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Whether data from the source is available (meaning it is already
        /// in the persisted data)
        /// </summary>
        public PersistedDataSourceStatus Status { get; set; }

        public long SerializedSize { get; private set; }

        public void Write(BufferWriter writer)
        {
            var startLength = writer.BytesWritten;
            writer.WriteString(this.Name);
            writer.WriteVariableLengthInt32((int)this.Status);
            this.SerializedSize = writer.BytesWritten - startLength;
        }
    }
}
