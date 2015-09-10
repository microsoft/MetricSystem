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

namespace MetricSystem.Utilities.UnitTests
{
    using System;

    using NUnit.Framework;

    [TestFixture]
    public sealed class GlobTests
    {
        [Test]
        public void NullStringHandling()
        {
            Assert.IsFalse(((string)null).MatchGlob(string.Empty));
            Assert.IsFalse(((string)null).MatchGlob("foo"));

            Assert.Throws<ArgumentNullException>(() => ((string)null).MatchGlob(null));
            Assert.Throws<ArgumentNullException>(() => "foo".MatchGlob(null));
        }

        [Test]
        public void LiteralMatches()
        {
            Assert.IsTrue("foo".MatchGlob("foo"));
            Assert.IsFalse(string.Empty.MatchGlob("foo"));
            Assert.IsFalse("foo".MatchGlob("fooo"));
            Assert.IsFalse("foo".MatchGlob("fo"));
            Assert.IsFalse("foo".MatchGlob(string.Empty));
        }

        [Test]
        public void CaseSensitive()
        {
            Assert.IsTrue("foo".MatchGlob("FOO"));
            Assert.IsTrue("foo".MatchGlob("FOO", ignoreCase: true));
            Assert.IsFalse("foo".MatchGlob("FOO", ignoreCase: false));

            Assert.IsTrue("foo".MatchGlob("F\\OO"));
            Assert.IsTrue("foo".MatchGlob("F\\OO", ignoreCase: true));
            Assert.IsFalse("foo".MatchGlob("F\\OO", ignoreCase: false));
        }

        [Test]
        public void StarMatches()
        {
            Assert.IsTrue(string.Empty.MatchGlob("*"));
            Assert.IsTrue("foo".MatchGlob("*"));
            Assert.IsTrue("foo".MatchGlob("**"));
            Assert.IsTrue("foo".MatchGlob("f*"));
            Assert.IsTrue("foo".MatchGlob("*f*"));
            Assert.IsTrue("foo".MatchGlob("fo*"));
            Assert.IsTrue("foo".MatchGlob("*fo*"));
            Assert.IsTrue("foo".MatchGlob("*f*o*"));
            Assert.IsTrue("foo".MatchGlob("foo*"));
            Assert.IsTrue("foo".MatchGlob("*foo*"));
            Assert.IsTrue("foo".MatchGlob("*f*o*o*"));

            Assert.IsFalse("foo".MatchGlob("fooo*"));
            Assert.IsFalse("foo".MatchGlob("*ooo*"));
        }

        [Test]
        public void QuestionMatches()
        {
            Assert.IsTrue("foo".MatchGlob("???"));
            Assert.IsTrue("foo".MatchGlob("f??"));
            Assert.IsTrue("foo".MatchGlob("?o?"));
            Assert.IsTrue("foo".MatchGlob("??o"));
            Assert.IsTrue("foo".MatchGlob("fo?"));
            Assert.IsTrue("foo".MatchGlob("f?o"));
            Assert.IsTrue("foo".MatchGlob("fo?"));

            Assert.IsFalse("foo".MatchGlob("?"));
            Assert.IsFalse("foo".MatchGlob("??"));
            Assert.IsFalse("foo".MatchGlob("????"));
        }

        [Test]
        public void MixedMatches()
        {
            foreach (var basicPat in new[] {"???", "f??", "?o?", "??o", "fo?", "f?o", "fo?"})
            {
                Assert.IsTrue("foo".MatchGlob("*" + basicPat));
                Assert.IsTrue("foo".MatchGlob(basicPat + "*"));
                Assert.IsTrue("foo".MatchGlob("*" + basicPat + "*"));
            }
        }

        [Test]
        public void EscapedCharacterMatches()
        {
            // Anything ending with a backtick is invalid
            Assert.IsFalse("foo".MatchGlob("*\\"));
            Assert.IsFalse("foo".MatchGlob("fo\\"));
            Assert.IsFalse("foo".MatchGlob("foo\\"));
            
            Assert.IsFalse(string.Empty.MatchGlob("\\*"));

            Assert.IsTrue("foo".MatchGlob("\\foo"));
            Assert.IsTrue("foo".MatchGlob("\\foo*"));
            Assert.IsTrue("foo".MatchGlob("*\\foo"));
            Assert.IsTrue("foo".MatchGlob("f\\oo"));
            Assert.IsTrue("foo".MatchGlob("fo\\o"));

            Assert.IsTrue("fo?".MatchGlob("fo\\?"));
            Assert.IsFalse("foo".MatchGlob("fo\\?"));
            Assert.IsFalse("fo?".MatchGlob("fo\\o"));

            Assert.IsTrue("fo*".MatchGlob("fo\\*"));
            Assert.IsFalse("fo*".MatchGlob("fo\\?"));
            Assert.IsFalse("fo*".MatchGlob("fo\\o"));
        }
    }
}
