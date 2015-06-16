// ---------------------------------------------------------------------
// <copyright file="DimensionSpecification.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
