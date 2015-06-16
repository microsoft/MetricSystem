// ---------------------------------------------------------------------
// <copyright file="ProtocolTests.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

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
