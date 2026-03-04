using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Arrow.Adbc.Drivers.Snowflake
{
    /// <summary>
    /// Helper extensions for creating loggers with a safe NullLogger fallback.
    /// </summary>
    public static class LoggerFactoryExtensions
    {
        /// <summary>
    /// Creates an <see cref="ILogger{T}"/> from the provided <see cref="ILoggerFactory"/>,
    /// or returns a <see cref="NullLogger{T}.Instance"/> when the factory is null.
    /// </summary>
    public static ILogger<T> CreateLogger<T>(this ILoggerFactory? factory)
    {
        return factory?.CreateLogger<T>() ?? NullLogger<T>.Instance;
    }
    }
}
