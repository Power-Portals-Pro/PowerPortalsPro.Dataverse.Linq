using Microsoft.Xrm.Sdk;

namespace PowerPortalsPro.Dataverse.Linq;

internal enum SlotKind
{
    /// <summary>Extract the root entity as a typed entity via ToEntity&lt;T&gt;()</summary>
    TypedEntity,
    /// <summary>Extract a linked entity from AliasedValue entries (may be null for left joins)</summary>
    LinkedEntity,
    /// <summary>Extract a single attribute from the root entity</summary>
    RootAttribute,
    /// <summary>Extract a single aliased value (linked entity attribute or aggregate result)</summary>
    AliasedValue,
}

internal sealed class MaterializerSlot
{
    public required SlotKind Kind { get; init; }
    public required Type ValueType { get; init; }
    public string? Alias { get; init; }
    public string? AttributeName { get; init; }
}

internal sealed class MaterializerInfo
{
    public required Delegate CompiledProjector { get; init; }
    public required Type ResultType { get; init; }
    public required MaterializerSlot[] Slots { get; init; }

    /// <summary>
    /// Invokes the materializer for a single entity result row.
    /// Extracts parameter values from the entity based on slots,
    /// then calls the compiled projector.
    /// </summary>
    public object Invoke(Entity entity)
    {
        var args = new object?[Slots.Length];
        for (var i = 0; i < Slots.Length; i++)
        {
            var slot = Slots[i];
            args[i] = slot.Kind switch
            {
                SlotKind.TypedEntity => ExtractTypedEntity(entity, slot.ValueType),
                SlotKind.LinkedEntity => ExtractLinkedEntity(entity, slot.Alias!, slot.ValueType),
                SlotKind.RootAttribute => AggregateProjection.ExtractRootValueUntyped(entity, slot.AttributeName!, slot.ValueType),
                SlotKind.AliasedValue => AggregateProjection.ExtractValueUntyped(entity, slot.Alias!, slot.ValueType),
                _ => throw new InvalidOperationException($"Unknown slot kind: {slot.Kind}")
            };
        }
        return CompiledProjector.DynamicInvoke(args)!;
    }

    private static object ExtractTypedEntity(Entity entity, Type entityType)
    {
        // Entity.ToEntity<T>() via reflection for arbitrary entity types
        return typeof(Entity).GetMethod(nameof(Entity.ToEntity))!
            .MakeGenericMethod(entityType)
            .Invoke(entity, null)!;
    }

    private static object? ExtractLinkedEntity(Entity entity, string alias, Type entityType)
    {
        // Reconstruct linked entity from aliased values, or null for unmatched left joins
        return typeof(AggregateProjection).GetMethod(nameof(AggregateProjection.ExtractLinkedEntity))!
            .MakeGenericMethod(entityType)
            .Invoke(null, [entity, alias]);
    }
}
