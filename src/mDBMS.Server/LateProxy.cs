using System.Reflection;

public class LateProxy<T> : DispatchProxy where T : class
{
    private T? _target;

    public static T Create()
    {
        return Create<T, LateProxy<T>>();
    }

    public void SetTarget(T target)
    {
        _target = target;
    }

    private T GetTargetOrThrow()
    {
        if (_target == null)
            throw new InvalidOperationException($"Target for proxy of {typeof(T).Name} is not set yet.");
        return _target;
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        var instance = GetTargetOrThrow();
        return targetMethod?.Invoke(instance, args);
    }
}
