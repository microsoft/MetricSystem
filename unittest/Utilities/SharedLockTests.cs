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
