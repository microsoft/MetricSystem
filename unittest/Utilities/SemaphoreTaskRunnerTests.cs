// ---------------------------------------------------------------------
// <copyright file="SemaphoreTaskRunnerTests.cs" company="Microsoft">
//       Copyright 2015 (c) Microsoft Corporation. All Rights Reserved.
//       Information Contained Herein is Proprietary and Confidential.
// </copyright>
// ---------------------------------------------------------------------

namespace MetricSystem.Utilities.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    using NUnit.Framework;

    [TestFixture]
    public class SemaphoreTaskRunnerTests
    {
        [Test]
        public void FailingJoinRunnerIsStillValid()
        {
            var value = 0;
            var waitForMe = new ManualResetEventSlim(false);

            var runner = new SemaphoreTaskRunner(2);

            runner.Schedule(waitForMe.Wait);
            for (var i = 0; i < 10; i++)
            {
                runner.Schedule(() => Interlocked.Increment(ref value));
            }

            Assert.IsFalse(runner.Join(TimeSpan.FromMilliseconds(100)));
            Assert.IsFalse(runner.Join(TimeSpan.FromMilliseconds(100)));

            waitForMe.Set();
            runner.Dispose();
            Assert.AreEqual(10, value);
        }

        [Test]
        public void ResetWithTasksPendingFails()
        {
            var waitForMe = new ManualResetEventSlim(false);
            var runner = new SemaphoreTaskRunner(2);

            runner.Schedule(waitForMe.Wait);

            try
            {
                runner.Reset();
                Assert.Fail("Reset should fail as a task is pending");
            }
            catch (InvalidOperationException) { }

            waitForMe.Set();
            runner.Dispose();
        }

        [Test]
        public void TaskRunnerConstructorChecksArgument()
        {
            try
            {
                new SemaphoreTaskRunner(0);
                Assert.Fail();
            }
            catch (ArgumentException) { }
        }

        [Test]
        public void TaskRunnerDisposeWaitsForTasksToFinish()
        {
            var value = 0;
            Action incrementValue = () => { value++; };

            var runner = new SemaphoreTaskRunner(1);
            const int iterations = 10;
            for (var i = 0; i < iterations; i++)
            {
                runner.Schedule(incrementValue);
            }

            // call dispose first
            runner.Dispose();
            Assert.AreEqual(iterations, value);
        }

        [Test]
        public void TaskRunnerExecutesAsynchronously()
        {
            var threadSet = new HashSet<int>();
            Action gimmeTheThread = () =>
                                    {
                                        lock (threadSet)
                                        {
                                            threadSet.Add(Thread.CurrentThread.ManagedThreadId);
                                        }
                                    };

            using (var runner = new SemaphoreTaskRunner(1))
            {
                for (var i = 0; i < 20; i++)
                {
                    runner.Schedule(gimmeTheThread);
                }

                runner.Join();
            }

            Assert.IsTrue(threadSet.Count > 2);
        }

        [Test]
        public void TaskRunnerHandlesLeakedExceptionsProperly()
        {
            var value = 0;
            Action incrementValue = () => { value++; };

            using (var runner = new SemaphoreTaskRunner(1))
            {
                // make sure the exception is properly handled and the runner is still valid
                for (var i = 0; i < 10; i++)
                {
                    try
                    {
                        runner.RunAsync(() => { throw new InvalidOperationException("tacos"); }).Wait();
                    }
                    catch (AggregateException e)
                    {
                        Assert.AreEqual(typeof(InvalidOperationException), e.InnerException.GetType());
                    }
                }

                var tasks = new Task[100];
                for (var i = 0; i < tasks.Length; i++)
                {
                    tasks[i] = runner.RunAsync(incrementValue);
                }

                Task.WaitAll(tasks);
                Assert.AreEqual(tasks.Length, value);
            }
        }

        [Test]
        public void TaskRunnerJoinAndResetRevivesTheObject()
        {
            var value = 0;
            Action incrementValue = () => { value++; };

            var runner = new SemaphoreTaskRunner(1);
            const int iterations = 10;
            for (var i = 0; i < iterations; i++)
            {
                runner.Schedule(incrementValue);
            }

            // join blocks until all tasks are done
            runner.Join(TimeSpan.FromMilliseconds(-1));
            Assert.AreEqual(iterations, value);

            //now the object is invalid
            try
            {
                runner.RunAsync(incrementValue).Wait();
            }
            catch (AggregateException) { }

            //reset brings it back to life and we are good to go again
            runner.Reset();
            value = 0;
            runner.Schedule(incrementValue);
            runner.Dispose();
            //dispose waited, so we're good
            Assert.AreEqual(1, value);
        }

        [Test]
        public void TaskRunnerLetsAllWorkHappen()
        {
            var value = 0;
            Action incrementValue = () =>
                                    {
                                        value++;
                                        Thread.Sleep(100);
                                    };

            using (var runner = new SemaphoreTaskRunner(1))
            {
                var tasks = new Task[10];
                for (var i = 0; i < tasks.Length; i++)
                {
                    tasks[i] = runner.RunAsync(incrementValue);
                }

                Task.WaitAll(tasks);
                Assert.AreEqual(tasks.Length, value);
            }

            value = 0;
            using (var runner = new SemaphoreTaskRunner(1))
            {
                var tasks = new Task[10];
                for (var i = 0; i < tasks.Length; i++)
                {
                    tasks[i] = runner.RunAsyncNonBlocking(incrementValue);
                }

                Task.WaitAll(tasks);
                Assert.IsTrue(tasks.Length > value); // Increments are not yet finished
            }
        }

        [Test]
        public void TaskRunnerMethodsCheckArgument()
        {
            using (var runner = new SemaphoreTaskRunner(2))
            {
                try
                {
                    runner.RunAsync<int>(null).Wait();
                    Assert.Fail();
                }
                catch (AggregateException e)
                {
                    Assert.AreEqual(typeof(ArgumentNullException), e.InnerException.GetType());
                }
                try
                {
                    runner.RunAsyncNonBlocking<int>(null).Wait();
                    Assert.Fail();
                }
                catch (AggregateException e)
                {
                    Assert.AreEqual(typeof(ArgumentNullException), e.InnerException.GetType());
                }
            }
        }

        [Test]
        public void TestNestedScheduling()
        {
            var value = 0;
            var runner = new SemaphoreTaskRunner(16);
            Action nestedIncrement = () =>
                                     {
                                         if (Interlocked.Increment(ref value) % 2 == 0)
                                         {
                                             runner.Schedule(() => Interlocked.Decrement(ref value));
                                         }
                                     };
            const int iterations = 100;
            for (var i = 0; i < iterations; i++)
            {
                runner.ScheduleNonBlocking(nestedIncrement);
            }
            Assert.IsTrue(runner.Join(TimeSpan.FromMilliseconds(1000)));
            Assert.IsTrue(value < 90);
            // picked up 90 as a safe number as this value will vary near 50 but not deterministic
        }

        [Test]
        public void TestNoWaitForScheduling()
        {
            var value = 0;
            Action delayedIncrement = () =>
                                      {
                                          Thread.Sleep(100);
                                          Interlocked.Increment(ref value);
                                      };

            var runner = new SemaphoreTaskRunner(16);
            const int iterations = 100;
            for (var i = 0; i < iterations; i++)
            {
                runner.ScheduleNonBlocking(delayedIncrement);
            }
            Assert.IsTrue(value < iterations);
            Assert.IsTrue(runner.Join(TimeSpan.FromMilliseconds(1000)));
            Assert.AreEqual(iterations, value);
        }
    }
}
