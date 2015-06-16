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
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Text;

    public sealed class DimensionSpecification : IDictionary<string, string>
    {
        private readonly IDictionary<string, string> dimensions;

        public DimensionSpecification()
        {
            this.dimensions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        public DimensionSpecification(IDictionary<string, string> source)
        {
            this.dimensions = new Dictionary<string, string>(source, StringComparer.OrdinalIgnoreCase);
        }

        public bool ContainsKey(string key)
        {
            return this.dimensions.ContainsKey(key);
        }

        public void Add(string key, string value)
        {
            this.dimensions.Add(key, value);
        }

        public bool Remove(string key)
        {
            return this.dimensions.Remove(key);
        }

        public void Add(KeyValuePair<string, string> item)
        {
            this.dimensions.Add(item);
        }

        public void Clear()
        {
            this.dimensions.Clear();
        }

        public bool Contains(KeyValuePair<string, string> item)
        {
            return this.dimensions.Contains(item);
        }

        public void CopyTo(KeyValuePair<string, string>[] array, int arrayIndex)
        {
            this.dimensions.CopyTo(array, arrayIndex);
        }

        public bool Remove(KeyValuePair<string, string> item)
        {
            return this.dimensions.Remove(item);
        }

        public int Count
        {
            get { return this.dimensions.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool TryGetValue(string key, out string value)
        {
            return this.dimensions.TryGetValue(key, out value);
        }

        public string this[string key]
        {
            get { return this.dimensions[key]; }
            set { this.dimensions[key] = value; }
        }

        ICollection<string> IDictionary<string, string>.Keys
        {
            get { return this.dimensions.Keys; }
        }

        public ICollection<string> Values
        {
            get { return this.dimensions.Values; }
        }

        public IEnumerable<string> Keys { get { return this.dimensions.Keys; } }

        internal Dictionary<string, string> Data { get { return this.dimensions as Dictionary<string, string>; } }
        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            return this.dimensions.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.dimensions.GetEnumerator();
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            foreach (var kvp in this)
            {
                sb.AppendFormat("{0}{1}: {2}", (sb.Length > 0 ? ", " : string.Empty), kvp.Key, kvp.Value);
            }

            return sb.ToString();
        }
    }
}
