using OutboxPattern.Infrastructure.Outbox;

namespace OutboxPattern.UnitTests;

public class BackoffCalculatorTests
{
    private static readonly TimeSpan BaseDelay = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromMinutes(5);

    [Theory]
    [InlineData(1, 2.0)]
    [InlineData(2, 4.0)]
    [InlineData(3, 8.0)]
    [InlineData(5, 32.0)]
    public void Delay_DoublesPerRetry_WithUpTo20PercentJitter(int retryCount, double expectedSeconds)
    {
        for (var i = 0; i < 1_000; i++)
        {
            var delay = BackoffCalculator.GetDelay(retryCount, BaseDelay, MaxDelay);

            Assert.InRange(delay.TotalSeconds, expectedSeconds, expectedSeconds * 1.2);
        }
    }

    [Theory]
    [InlineData(9)]
    [InlineData(30)]
    [InlineData(10_000)]
    public void Delay_IsCappedAtMaxDelay(int retryCount)
    {
        for (var i = 0; i < 1_000; i++)
        {
            var delay = BackoffCalculator.GetDelay(retryCount, BaseDelay, MaxDelay);

            Assert.Equal(MaxDelay, delay);
        }
    }

    [Fact]
    public void Delay_StaysWithinBaseAndMaxForAnyRetry()
    {
        for (var retryCount = 0; retryCount <= 50; retryCount++)
        {
            var delay = BackoffCalculator.GetDelay(retryCount, BaseDelay, MaxDelay);

            Assert.InRange(delay, BaseDelay, MaxDelay);
        }
    }
}
