using P = Runwick.Management.ManagementPermissions;

namespace Runwick.Dashboard.Pages;

internal sealed class StorePage : DashboardPage
{
    public static readonly StorePage Instance = new();

    public override string Component => "Store";

    public override string Permission => P.SettingsRead;

    protected override bool SupportsNotifications => false;

    public override IReadOnlyDictionary<string, string> Props { get; } = Pages.Props.Of(("snapshot", P.SettingsRead));

    protected override async Task<object?> LoadAsync(DashboardRequest r, string prop) =>
        await r.Session.GetStoreSnapshotAsync(r.Caller, r.Ct);
}
