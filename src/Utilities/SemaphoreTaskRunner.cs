// ---------------------------------------------------------------------
// <copyright file="SemaphoreTaskRunner.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Utilities
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Small wrapper around a semaphore that only lets N async tasks run in parallel.
    /// </summary>
    public sealed class SemaphoreTaskRunner : IDisposable
    {
        private CountdownEvent pendingTasksEvent;
        private SemaphoreSlim restrictor;

        /// <summary>
        /// Creates a task runner which allows only a certain number of tasks to execute simultaneously.
        /// </summary>
        /// <param name="maxParalleism">Maximum number of tasks which may run in parallel.</param>
        public SemaphoreTaskRunner(int maxParalleism)
        {
            if (maxParalleism <= 0)
            {
                throw new ArgumentOutOfRangeException("maxParalleism");
            }

            this.restrictor = new SemaphoreSlim(maxParalleism);
            this.pendingTasksEvent = new CountdownEvent(1);
        }

        /// <summary>
        /// Retrieve the current number of running tasks.
        /// </summary>
        public int CurrentCount
        {
            get { return this.pendingTasksEvent.CurrentCount - 1; }
        }

        /// <summary>
        /// Release resources and wait for all tasks to finish
        /// </summary>
        public void Dispose()
        {
            if (!this.pendingTasksEvent.IsSet)
            {
                this.Join();
            }

            this.pendingTasksEvent.Dispose();
            this.pendingTasksEvent = null;

            if (this.restrictor != null)
            {
                this.restrictor.Dispose();
                this.restrictor = null;
            }
        }

        /// <summary>
        /// Wait for all outstanding tasks to complete. To use the runner again you must call <see cref="Reset"/>.
        /// </summary>
        public void Join()
        {
            this.Join(TimeSpan.FromMilliseconds(-1));
        }

        /// <summary>
        /// Wait for the specified amount of time for all outstanding tasks to complete. To use the runner again you
        /// must call <see cref="Reset"/>.
        /// </summary>
        /// <param name="waitTime">Maximum time to wait (or -1 milliseconds for infinity).</param>
        public bool Join(TimeSpan waitTime)
        {
            this.ValidateState();

            this.pendingTasksEvent.Signal();
            if (this.pendingTasksEvent.Wait(waitTime))
            {
                return true;
            }

            // re-add the one we removed so join can be called again
            this.pendingTasksEvent.AddCount();
            return false;
        }

        private void ValidateState()
        {
            if (this.pendingTasksEvent == null || this.restrictor == null)
            {
                throw new ObjectDisposedException("SemaphoreTaskRunner");
            }

            if (this.pendingTasksEvent.IsSet)
            {
                throw new InvalidOperationException("Join has been called without a subsequent call to Reset");
            }
        }

        /// <summary>
        /// Reset the runner to be useable after a call to Join 
        /// </summary>
        public void Reset()
        {
            if (this.pendingTasksEvent.CurrentCount > 1)
            {
                throw new InvalidOperationException(
                    "Cannot reset while there are pending tasks. Must join or wait for tasks to drain first");
            }
            this.pendingTasksEvent.Reset();
        }

        /// <summary>
        /// Run the given action asynchronously. Blocks if the maximum number of parallel actions are already running.
        /// Exceptions are leaked and up to the caller to handle.
        /// </summary>
        /// <param name="action">The action to run.</param>
        public async Task RunAsync(Action action)
        {
            if (action == null)
            {
                throw new ArgumentNullException("action");
            }

            this.ValidateState();
            this.pendingTasksEvent.AddCount(1);

            await Task.Factory.StartNew(() =>
                                        {
                                            this.restrictor.Wait();
                                            try
                                            {
                                                action();
                                            }
                                            finally
                                            {
                                                this.restrictor.Release();
                                                this.pendingTasksEvent.Signal();
                                            }
                                        });
        }

        /// <summary>
        /// Run the given function asynchronously. Blocks if the maximum number of parallel actions are already running.
        /// Exceptions are leaked and up to the caller to handle.
        /// </summary>
        /// <param name="func">The function to run.</param>
        public async Task<TResult> RunAsync<TResult>(Func<TResult> func)
        {
            if (func == null)
            {
                throw new ArgumentNullException("func");
            }

            this.ValidateState();
            this.pendingTasksEvent.AddCount();

            return await Task.Factory.StartNew(() =>
                                               {
                                                   this.restrictor.Wait();

                                                   try
                                                   {
                                                       return func();
                                                   }
                                                   finally
                                                   {
                                                       this.restrictor.Release();
                                                       this.pendingTasksEvent.Signal();
                                                   }
                                               });
        }

        /// <summary>
        /// Run the given action asynchronously. Async waits if the maximum number of parallel actions are already running. Doesn't block threads.
        /// Exceptions are leaked and up to the caller to handle.
        /// </summary>
        /// <param name="action">The action to run.</param>
        public async Task RunAsyncNonBlocking(Action action)
        {
            if (action == null)
            {
                throw new ArgumentNullException("action");
            }

            this.ValidateState();
            this.pendingTasksEvent.AddCount(1);

            await Task.Factory.StartNew(() => this.DoAction(action));
        }

        /// <summary>
        /// Run the given function asynchronously. Async waits if the maximum number of parallel actions are already running. Doesn't block threads.
        /// Exceptions are leaked and up to the caller to handle.
        /// </summary>
        /// <param name="func">The function to run.</param>
        public async Task<TResult> RunAsyncNonBlocking<TResult>(Func<TResult> func)
        {
            if (func == null)
            {
                throw new ArgumentNullException("func");
            }

            this.ValidateState();
            this.pendingTasksEvent.AddCount();

            var task = await Task.Factory.StartNew(() => this.DoAction(func));

            return task.Result;
        }

        /// <summary>
        /// Schedule the given action to run asynchronously. Does not block this thread but underlying task threads can be blocked
        /// Exceptions are leaked, and will cause an unhandled exception error within the application.
        /// </summary>
        /// <param name="action">The action to run.</param>
        public async void Schedule(Action action)
        {
            await this.RunAsync(action);
        }

        /// <summary>
        /// Schedule the given action to run asynchronously. Does not block.
        /// Exceptions are leaked, and will cause an unhandled exception error within the application.
        /// </summary>
        /// <param name="action">The action to run.</param>
        public async void ScheduleNonBlocking(Action action)
        {
            await this.RunAsync(action);
        }

        /// <summary>
        /// Async Action processing task which will not block any thread
        /// </summary>
        /// <param name="action"></param>
        private async void DoAction(Action action)
        {
            await this.restrictor.WaitAsync();
            try
            {
                action();
            }
            finally
            {
                this.restrictor.Release();
                this.pendingTasksEvent.Signal();
            }
        }

        /// <summary>
        /// Async func execution which will not block any thread
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="func"></param>
        /// <returns></returns>
        private async Task<TResult> DoAction<TResult>(Func<TResult> func)
        {
            await this.restrictor.WaitAsync();
            try
            {
                return func();
            }
            finally
            {
                this.restrictor.Release();
                this.pendingTasksEvent.Signal();
            }
        }
    }
}
