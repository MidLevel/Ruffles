using System;
using System.Reflection;

namespace Ruffles.Utils
{
    /// <summary>
    /// Logging utils.
    /// </summary>
    public static class Logging
    {
        /// <summary>
        /// Whether or not to try to auto hook common loggers such as UnityEngine.Debug
        /// </summary>
        public static bool TryAutoHookCommonLoggers = true;
        
        /// <summary>
        /// Gets the current log level.
        /// </summary>
        /// <value>The current log level.</value>
        public static LogLevel CurrentLogLevel = LogLevel.Info;
        
        /// <summary>
        /// Occurs when ruffles spits out an info log.
        /// </summary>
        public static event Action<string> OnInfoLog = (value) => Console.WriteLine("[INFO] " + value);
        /// <summary>
        /// Occurs when ruffles spits out a warning log.
        /// </summary>
        public static event Action<string> OnWarningLog = (value) => Console.WriteLine("[WARNING] " + value);
        /// <summary>
        /// Occurs when ruffles spits out an error log.
        /// </summary>
        public static event Action<string> OnErrorLog = (value) => Console.WriteLine("[ERROR] " + value);

        internal static void LogInfo(string value)
        {
            if (OnInfoLog != null)
            {
                OnInfoLog(value);
            }
        }

        internal static void LogWarning(string value)
        {
            if (OnWarningLog != null)
            {
                OnWarningLog(value);
            }
        }

        internal static void LogError(string value)
        {
            if (OnErrorLog != null)
            {
                OnErrorLog(value);
            }
        }

        static Logging()
        {
            if (TryAutoHookCommonLoggers)
            {
                TryHookUnityEngineLoggers();
            }
        }
        
        private static void TryHookUnityEngineLoggers()
        {
            if (!TypeUtils.TryGetTypeByName("UnityEngine.Debug, UnityEngine.CoreModule", out var unityDebugType))
            {
                return;
            }

            if (unityDebugType.TryGetMethod("Log", BindingFlags.Static | BindingFlags.Public, new Type[] {typeof(object)}, out var infoLogMethod))
            {
                OnInfoLog += (value) => { infoLogMethod.Invoke(null, new object[] {value}); };
                if (CurrentLogLevel <= LogLevel.Debug) LogInfo("UnityEngine.Debug.Log(object) was hooked");
            }

            if (unityDebugType.TryGetMethod("LogWarning", BindingFlags.Static | BindingFlags.Public, new Type[] {typeof(object)}, out var warningLogMethod))
            {
                OnWarningLog += (value) => { warningLogMethod.Invoke(null, new object[] {value}); };
                if (CurrentLogLevel <= LogLevel.Debug) LogInfo("UnityEngine.Debug.LogWarning(object) was hooked");
            }

            if (unityDebugType.TryGetMethod("LogError", BindingFlags.Static | BindingFlags.Public, new Type[] {typeof(object)}, out var errorLogMethod))
            {
                OnErrorLog += (value) => { errorLogMethod.Invoke(null, new object[] {value}); };
                if (CurrentLogLevel <= LogLevel.Debug) LogInfo("UnityEngine.Debug.LogError(object) was hooked");
            }
        }
    }
}