using Microsoft.Xrm.Sdk;
using PowerPortalsPro.Dataverse.Linq.Expressions;
using System.Linq.Expressions;

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

    /// <summary>
    /// For a <see cref="SlotKind.LinkedEntity"/>: its primary key attribute, from metadata, which
    /// fills its <see cref="Entity.Id"/>. Null when it couldn't be read; <c>{logicalname}id</c> is
    /// tried then.
    /// </summary>
    public string? PrimaryKey { get; init; }
}

internal sealed class MaterializerInfo
{
    public required Delegate CompiledProjector { get; init; }
    /// <summary>
    /// The type <see cref="Invoke"/> returns: the projector's result, or the last
    /// <see cref="Then"/> select's.
    /// </summary>
    public required Type ResultType { get; init; }
    public required MaterializerSlot[] Slots { get; init; }

    /// <summary>
    /// Selects run on the projector's result, in order, for a select on an earlier projection
    /// (<c>.Select(a =&gt; a.Name).Take(5).Select(n =&gt; n.ToUpper())</c>). Null when there are none.
    /// </summary>
    public Func<object?, object?>? Then { get; init; }

    /// <summary>
    /// This materializer with <paramref name="select"/> run on each of its results.
    /// </summary>
    public MaterializerInfo ThenSelect(LambdaExpression select)
    {
        // Compiled as object -> object, so each row calls it directly rather than by reflection.
        var value = Expression.Parameter(typeof(object), "value");
        var body = select.Body.ReadIndexerAsAttributeValue().ReplaceParameter(
            select.Parameters[0], Expression.Convert(value, select.Parameters[0].Type));
        var then = Expression.Lambda<Func<object?, object?>>(
            Expression.Convert(body, typeof(object)), value).Compile();

        var previous = Then;
        return new MaterializerInfo
        {
            CompiledProjector = CompiledProjector,
            ResultType = select.ReturnType,
            Slots = Slots,
            Then = previous is null ? then : row => then(previous(row)),
        };
    }

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
                SlotKind.LinkedEntity => ExtractLinkedEntity(entity, slot.Alias!, slot.ValueType, slot.PrimaryKey),
                SlotKind.RootAttribute => AggregateProjection.ExtractRootValueUntyped(entity, slot.AttributeName!, slot.ValueType),
                SlotKind.AliasedValue => AggregateProjection.ExtractValueUntyped(entity, slot.Alias!, slot.ValueType),
                _ => throw new InvalidOperationException($"Unknown slot kind: {slot.Kind}")
            };
        }
        var projected = CompiledProjector.DynamicInvoke(args);
        return (Then is null ? projected : Then(projected))!;
    }

    private static object ExtractTypedEntity(Entity entity, Type entityType)
    {
        // Entity.ToEntity<T>() via reflection for arbitrary entity types
        return typeof(Entity).GetMethod(nameof(Entity.ToEntity))!
            .MakeGenericMethod(entityType)
            .Invoke(entity, null)!;
    }

    private static object? ExtractLinkedEntity(Entity entity, string alias, Type entityType, string? primaryKey)
    {
        // Reconstruct linked entity from aliased values, or null for unmatched left joins
        return typeof(AggregateProjection).GetMethod(nameof(AggregateProjection.ExtractLinkedEntity))!
            .MakeGenericMethod(entityType)
            .Invoke(null, [entity, alias, primaryKey]);
    }
}
