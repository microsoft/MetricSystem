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

namespace MetricSystem.Schemas.UnitTests
{
    using System;
    using System.Collections.Generic;

    using NUnit.Framework;

    [TestFixture]
    public sealed class ProtocolTests
    {
        [Test]
        public void IsValidCounterNameRules()
        {
            Assert.IsFalse(Protocol.IsValidCounterName(null));
            Assert.IsFalse(Protocol.IsValidCounterName(string.Empty));
            Assert.IsFalse(Protocol.IsValidCounterName("/"));
            Assert.IsFalse(Protocol.IsValidCounterName("/ "));
            Assert.IsFalse(Protocol.IsValidCounterName("/\t"));
            Assert.IsFalse(Protocol.IsValidCounterName("/   "));
            Assert.IsFalse(Protocol.IsValidCounterName("/foo "));
            Assert.IsFalse(Protocol.IsValidCounterName("foo"));
            Assert.IsFalse(Protocol.IsValidCounterName("/foo/"));
            Assert.IsFalse(Protocol.IsValidCounterName("/f\\oo"));
            Assert.IsTrue(Protocol.IsValidCounterName("/foo"));
        }

        [Test]
        public void BuildCounterRequestCommandOutputEmitsExpectedPaths()
        {
            Assert.Throws<ArgumentException>(() => Protocol.BuildCounterRequestCommand(null, null, null));
            Assert.Throws<ArgumentException>(() => Protocol.BuildCounterRequestCommand("foo", "not a valid counter", null));
            Assert.AreEqual("/counters/*/foo", Protocol.BuildCounterRequestCommand("foo", null, null));
            Assert.AreEqual("/counters/bar/baz/foo", Protocol.BuildCounterRequestCommand("foo", "/bar/baz", null));
            Assert.AreEqual("/counters/*/foo?taco=yum",
                            Protocol.BuildCounterRequestCommand("foo", null,
                                                                new Dictionary<string, string> {{"taco", "yum"}}));
            Assert.AreEqual("/counters/bar/baz/foo?taco=yum",
                            Protocol.BuildCounterRequestCommand("foo", "/bar/baz",
                                                                new Dictionary<string, string> {{"taco", "yum"}}));
        }
    }
}
