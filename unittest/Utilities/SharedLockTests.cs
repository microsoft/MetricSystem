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
    using System.Threading;

    using NUnit.Framework;

    [TestFixture]
    public class SharedLockTests
    {
        [Test]
        public void SharedLockValidatesParameters()
        {
            Assert.Throws<ArgumentNullException>(() => SharedLock.OpenShared(null));
        }

        [Test]
        public void SharedLockProtectsAgainstDoubleDispose()
        {
            using (var rwSlim = new ReaderWriterLockSlim())
            {
                var testMe = SharedLock.OpenExclusive(rwSlim);
                testMe.Dispose();
                Assert.Throws<ObjectDisposedException>(testMe.Dispose);
            }
        }

        [Test]
        public void SharedLockReleasesOnUnhandledException()
        {
            using (var rwSlim = new ReaderWriterLockSlim())
            {
                try
                {
                    using (var testMe = SharedLock.OpenShared(rwSlim))
                    {
                        Assert.AreEqual(1, rwSlim.CurrentReadCount);
                        throw new InvalidOperationException("Tacos must be delicious");
                    }
                }
                catch (InvalidOperationException) { }

                Assert.AreEqual(0, rwSlim.CurrentReadCount);
            }

        }

    }
}
