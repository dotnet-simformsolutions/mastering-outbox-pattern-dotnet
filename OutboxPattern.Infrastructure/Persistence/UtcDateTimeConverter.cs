using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace OutboxPattern.Infrastructure.Persistence;

public sealed class UtcDateTimeConverter() : ValueConverter<DateTime, DateTime>(
    value => value,
    value => DateTime.SpecifyKind(value, DateTimeKind.Utc));
