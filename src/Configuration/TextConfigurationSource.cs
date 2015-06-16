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
    /// Represents configuration backed by plaintext data. Sends updates when the data is updated.
    /// </summary>
    public sealed class TextConfigurationSource : ConfigurationSource
    {
        private string contents;

        /// <summary>
        /// Create text configuration with an empty JSON object.
        /// </summary>
        public TextConfigurationSource() : this(EmptyContent) { }

        /// <summary>
        /// Create text configuration with the given JSON text.
        /// </summary>
        /// <param name="contents">JSON to use as initial contents.</param>
        public TextConfigurationSource(string contents)
        {
            if (contents == null)
            {
                throw new ArgumentNullException("contents");
            }

            this.contents = contents;
        }

        /// <summary>
        /// The current contents of the source.
        /// </summary>
        public string Contents
        {
            get { return this.contents; }
            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException();
                }

                this.contents = value;
                this.SetUpdated();
            }
        }

        /// <summary>
        /// Name for this configuration source. Helpful when reporting syntactical/validation issues.
        /// </summary>
        public string Name { get; set; }

        protected override JsonReader GetReader()
        {
            return new JsonTextReader(new StringReader(this.Contents));
        }

        public override string ToString()
        {
            return this.Name;
        }
    }
}
