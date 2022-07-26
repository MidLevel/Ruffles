using System;
using System.Reflection;

namespace Ruffles.Utils
{
    internal static class TypeUtils
    {
        internal static bool TryGetMethod(this Type type, string name, BindingFlags flags, Type[] args, out MethodInfo method)
        {
            method = type.GetMethod(name, flags, null, args, null);
            return method != null;
        }

        internal static bool TryGetTypeByName(string name, out Type type)
        {
            try
            {
                type = Type.GetType(name);
                return type != null;
            }
            catch (TypeLoadException)
            {
                if (Logging.CurrentLogLevel <= LogLevel.Debug)
                {
                    Logging.LogInfo($"Could not load type '{name}'.");
                }

                type = null;
                return false;
            }
        }
    }
}