#if !NET5_0_OR_GREATER
using System.Collections.Generic;

// ReSharper disable once CheckNamespace
namespace System;

internal static class KeyValuePairExtensions
{
    public static void Deconstruct<TKey, TValue>(
        this KeyValuePair<TKey, TValue> pair, out TKey key, out TValue value)
    {
        key = pair.Key;
        value = pair.Value;
    }
}
#endif
