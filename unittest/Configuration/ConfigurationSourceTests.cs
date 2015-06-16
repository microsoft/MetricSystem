// ---------------------------------------------------------------------
// <copyright file="ConfigurationSourceTests.cs" company="Microsoft">
//       Copyright 2014 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Configuration.UnitTests
{
    using System;

    using Newtonsoft.Json;

    using NUnit.Framework;

    /// <summary>
    /// Summary description for ConfigurationSourceTests
    /// </summary>
    [TestFixture]
    public sealed class ConfigurationSourceTests
    {
        [Test]
        public void ContentUpdateWithoutHandlerIsSafe()
        {
            var textSource = new TextConfigurationSource();
            textSource.Contents = "{}";
        }

        [Test]
        public void ContentUpdateHandlerIsCalled()
        {
            var textSource = new TextConfigurationSource();
            var updated = false;
            textSource.SourceUpdated += () => updated = true;

            Assert.AreEqual(false, updated);
            textSource.Contents = string.Empty;
            Assert.AreEqual(true, updated);
        }

        [Test]
        public void ContentUpdateHandlerIsCalledForInvalidData()
        {
            var textSource = new TextConfigurationSource();
            var updated = false;
            textSource.SourceUpdated += () => updated = true;

            Assert.AreEqual(false, updated);
            textSource.Contents = "not valid JSON";
            Assert.AreEqual(true, updated);
        }

        [Test]
        public void TextConfigurationSourceContentCannotBeNull()
        {
            Assert.Throws<ArgumentNullException>(() => new TextConfigurationSource(null));

            Assert.Throws<ArgumentNullException>(() =>
                                                 {
                                                     var source = new TextConfigurationSource();
                                                     source.Contents = null;
                                                 });
        }

        [Test]
        public void TextConfigurationSourceCanReadDefaultContent()
        {
            Assert.IsNotNull(new TextConfigurationSource().Read());
        }

        [Test]
        public void TextConfigurationSourceThrowsWhenReadingInvalidData()
        {
            Assert.Throws<JsonReaderException>(() => new TextConfigurationSource("not valid").Read());
        }
    }
}
