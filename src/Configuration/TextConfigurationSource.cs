// ---------------------------------------------------------------------
// <copyright file="TextConfigurationSource.cs" company="Microsoft">
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
