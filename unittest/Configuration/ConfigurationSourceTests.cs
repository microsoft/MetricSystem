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
