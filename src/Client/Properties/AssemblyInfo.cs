using System.Runtime.CompilerServices;

// any assembly that needs to access internals such as mocking/overriding the HttpRequesterFactory can go here
[assembly: InternalsVisibleTo("MetricSystem.Client.UnitTests")]
[assembly: InternalsVisibleTo("MetricSystem.Server.UnitTests")]
