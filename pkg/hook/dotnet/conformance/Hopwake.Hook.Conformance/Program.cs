// Runs the hook conformance script (docs/hooks.md) with the .NET hook, under hookcheck:
//
//   bin/hookcheck -service conformance-dotnet -- dotnet pkg/hook/dotnet/conformance/Hopwake.Hook.Conformance/bin/Release/net10.0/Hopwake.Hook.Conformance.dll
using System;
using System.Threading.Tasks;
using Hopwake;

try
{
    await using var hook = new Hook(new HookOptions
    {
        Tenant = Environment.GetEnvironmentVariable("HOPWAKE_TENANT"),
        Hop = Environment.GetEnvironmentVariable("HOPWAKE_HOP"),
        Service = "conformance-dotnet",
    });
    var at = new DateTimeOffset(2026, 11, 18, 10, 0, 0, 750, TimeSpan.Zero);
    const string type = "ems.measurements.Measurement";

    hook.Mark(HopOp.Store, "dev-00042", 1683);
    hook.MarkBoot(HopOp.Store, "dev-00042", "7", 12);
    hook.Mark(HopOp.Serve, "dev-00042", 1683);
    hook.Mark(HopOp.Serve, "dev-00042", 1683); // the same reading served again: not sent
    hook.MarkTime(HopOp.Store, "EMS0000042", type, at);
    hook.MarkTimeTeach(HopOp.Store, "EMS0000042", "systemId", "100042", type, at);

    // The alias list arrives shortly after the hook starts.
    var deadline = DateTime.UtcNow.AddSeconds(15);
    while (!hook.MarkTimeAlias(HopOp.Serve, "systemId", "100042", type, at))
    {
        if (DateTime.UtcNow > deadline)
        {
            Console.Error.WriteLine("conformance: the alias systemId:100042 never became known");
            return 1;
        }
        await Task.Delay(100);
    }
    if (hook.MarkTimeAlias(HopOp.Serve, "systemId", "999999", type, at))
    {
        Console.Error.WriteLine("conformance: the unknown alias systemId:999999 was marked, want unknown");
        return 1;
    }
}
catch (Exception e)
{
    Console.Error.WriteLine("conformance: " + e.Message);
    return 1;
}
return 0;
