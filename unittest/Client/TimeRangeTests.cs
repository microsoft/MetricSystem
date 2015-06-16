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

namespace MetricSystem.Client.UnitTests
{
    using System;
    using System.Collections.Generic;

    using NUnit.Framework;

    [TestFixture]
    public class TimeRangeTests
    {
        [Test]
        public void TimeRangeConstructorChecksArguments()
        {
            Assert.Throws<ArgumentException>(() => new TimeRange(DateTime.Now, DateTime.Now.AddMinutes(-1)));
        }

        [Test]
        public void TimeRangeMergeChecksArguments()
        {
            Assert.Throws<ArgumentNullException>(() => TimeRange.Merge(null, new TimeRange(DateTime.Now, DateTime.Now)));
            Assert.Throws<ArgumentNullException>(() => TimeRange.Merge(new TimeRange(DateTime.Now, DateTime.Now), null));
        }

        [Test]
        public void TimeRangeMergeSucceeds()
        {
            var nowWithMs = DateTime.Now;
            var now = new DateTime(nowWithMs.Year, nowWithMs.Month, nowWithMs.Day, nowWithMs.Hour, nowWithMs.Minute, nowWithMs.Second, nowWithMs.Millisecond, DateTimeKind.Utc);
            var yesterday = now.AddDays(-1);
            var tomorrow = now.AddDays(1);

            var timeA = new TimeRange(yesterday, now);
            var timeB = new TimeRange(now, tomorrow);

            Assert.IsTrue(TimeRange.Merge(timeA, timeB).Start == yesterday);
            Assert.IsTrue(TimeRange.Merge(timeA, timeB).End == tomorrow);
            Assert.IsTrue(TimeRange.Merge(timeB, timeA).Start == yesterday);
            Assert.IsTrue(TimeRange.Merge(timeB, timeA).End == tomorrow);
            Assert.IsTrue(TimeRange.Merge(timeA, timeA).Equals(timeA));
        }

        [Test]
        public void TimeRangeEqualityCheck()
        {
            var timeA = DateTime.Now;
            var timeB = timeA.AddSeconds(100);

            var range = new TimeRange(timeA, timeB);
            var other = new TimeRange(timeA, timeB);
            var not = new TimeRange(timeA, timeB.AddSeconds(10));

            Assert.IsTrue(range.Equals(range));
            Assert.IsTrue(range.Equals(other));
            Assert.IsFalse(range.Equals(not));
            Assert.IsFalse(range.Equals(null));
            Assert.IsFalse(range.Equals(new object()));
        }

        [Test]
        public void TimeRangeComparisonCheck()
        {
            var now = DateTime.Now;

            var earliestStart = new TimeRange(now, now.AddSeconds(100));
            var middleStart = new TimeRange(now.AddSeconds(10), now.AddMinutes(10));
            var latestStart = new TimeRange(now.AddSeconds(100), now.AddSeconds(101));

            var sortedList = new SortedList<TimeRange, int>();
            sortedList.Add(latestStart, 3);
            sortedList.Add(middleStart, 2);
            sortedList.Add(earliestStart, 1);

            int lastSeen = 0;
            foreach (var pair in sortedList)
            {
                Assert.IsTrue(lastSeen < pair.Value);
                lastSeen = pair.Value;
            }
        }

        [Test]
        public void TestTimeRangeIntersectWithNoOverlap()
        {
            var now = DateTime.Now;
            var yesterday = now.AddDays(-1);

            var rangeA = new TimeRange(now, now.AddMinutes(1));
            var rangeB = new TimeRange(yesterday, yesterday.AddMinutes(1));

            Assert.IsFalse(rangeA.IntersectsWith(rangeB));
            Assert.IsFalse(rangeB.IntersectsWith(rangeA));
        }

        [Test]
        public void TestTimeRangeIntersectExactOverlap()
        {
            var now = DateTime.Now;
            var rangeA = new TimeRange(now, now.AddMinutes(1));

            Assert.IsTrue(rangeA.IntersectsWith(rangeA));
        }

        [Test]
        public void TestTimeRangeIntersectLeftOverlap()
        {
            var now = DateTime.Now;
            var rangeA = new TimeRange(now, now.AddMinutes(2));
            var rangeB = new TimeRange(now.AddMinutes(-1), now.AddMinutes(1));

            Assert.IsTrue(rangeA.IntersectsWith(rangeB));
            Assert.IsTrue(rangeB.IntersectsWith(rangeA));
        }

        [Test]
        public void TestTimeRangeIntersectRightOverlap()
        {
            var now = DateTime.Now;
            var rangeA = new TimeRange(now, now.AddMinutes(2));
            var rangeB = new TimeRange(now.AddMinutes(1), now.AddMinutes(10));

            Assert.IsTrue(rangeA.IntersectsWith(rangeB));
            Assert.IsTrue(rangeB.IntersectsWith(rangeA));
        }

        [Test]
        public void TestTimeRangeIntersectSupersetOverlap()
        {
            var now = DateTime.Now;
            var rangeA = new TimeRange(now, now.AddMinutes(2));
            var rangeB = new TimeRange(now.AddMinutes(-10), now.AddMinutes(10));

            Assert.IsTrue(rangeA.IntersectsWith(rangeB));
            Assert.IsTrue(rangeB.IntersectsWith(rangeA));
        }

        [Test]
        public void TestTimeRangeIntersectSubsetOverlap()
        {
            var now = DateTime.Now;
            var rangeA = new TimeRange(now, now.AddMinutes(20));
            var rangeB = new TimeRange(now.AddMinutes(5), now.AddMinutes(10));

            Assert.IsTrue(rangeA.IntersectsWith(rangeB));
            Assert.IsTrue(rangeB.IntersectsWith(rangeA));
        }

        [Test]
        public void TestTimeRangeIntersectImmediatelyAdjacent()
        {
            var now = DateTime.Now;
            var rangeA = new TimeRange(now, now.AddMinutes(2));
            var rangeB = new TimeRange(now.AddMinutes(-2), now);

            Assert.IsFalse(rangeA.IntersectsWith(rangeB));
            Assert.IsFalse(rangeB.IntersectsWith(rangeA));
        }

    }
}
