namespace MetricSystem.Utilities.UnitTests
{
    using System;
    using NUnit.Framework;

    [TestFixture]
    public class DateTimeExtensionMethodTests
    {
        [Test]
        public void CanConvertBothDirections()
        {
            var now = DateTime.UtcNow;

            var nowWithoutMs = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second,
                                            now.Millisecond, DateTimeKind.Utc);
            var rightNow = now.ToMillisecondTimestamp().ToDateTime();

            Assert.IsTrue(nowWithoutMs.Equals(rightNow));

            var yesterday = (nowWithoutMs.ToMillisecondTimestamp() - (long)TimeSpan.FromDays(1).TotalMilliseconds).ToDateTime();
            var dayAfterTomorrow = yesterday.AddDays(1);

            Assert.IsTrue(nowWithoutMs.Equals(dayAfterTomorrow.ToUniversalTime()));
        }

    }
}
