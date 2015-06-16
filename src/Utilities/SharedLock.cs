namespace MetricSystem.Utilities
{
    using System;
    using System.Threading;

    /// <summary>
    /// Wrapper around a ReaderWriterLockSlim that simplifies usage pattern and cleanup
    /// </summary>
    public sealed class SharedLock : IDisposable
    {
        private readonly ReaderWriterLockSlim internalLock;
        private readonly bool isRead = false;
        private bool isDisposed = false;

        public static SharedLock OpenShared(ReaderWriterLockSlim innerLock)
        {
            return new SharedLock(innerLock, true);
        }

        public static SharedLock OpenExclusive(ReaderWriterLockSlim innerLock)
        {
            return new SharedLock(innerLock, false);
        }

        private SharedLock(ReaderWriterLockSlim innerLock, bool isRead)
        {
            if (innerLock == null)
            {
                throw new ArgumentNullException("innerLock");
            }

            this.internalLock = innerLock;
            this.isRead = isRead;

            if (this.isRead)
            {
                this.internalLock.EnterReadLock();
            }
            else
            {
                this.internalLock.EnterWriteLock();
            }
        }

        public void Dispose()
        {
            if (this.isDisposed)
            {
                throw new ObjectDisposedException("double disposed");
            }

            this.isDisposed = true;

            if (this.isRead)
            {
                this.internalLock.ExitReadLock();
            }
            else
            {
                this.internalLock.ExitWriteLock();
            }
        }
    }
}
