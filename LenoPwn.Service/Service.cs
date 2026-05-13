using NAudio.CoreAudioApi;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Management;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.ServiceProcess;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace LenoPwn.Service
{
    [SupportedOSPlatform("windows")]
    public class LenoPwnService : ServiceBase
    {
        private const string PipeName = "LenoPwnPipe";
        private const string ConfigFileName = "hotkey_map.json";
        private const int DebouncingDelayMs = 100; // Debounce hotkey events
        private ManagementEventWatcher? _watcher;
        private CancellationTokenSource? _cancellationTokenSource;
        private NamedPipeClientStream? _pipeClient;
        private StreamWriter? _pipeWriter;
        private MMDeviceEnumerator? _deviceEnumerator;
        private MMDevice? _micDevice;
        private MMDevice? _speakerDevice;
        private string _currentTheme = "Dark";

        private AppConfig? _cachedConfig;
        private FileSystemWatcher? _configWatcher;
        private readonly object _configLock = new object();
        private readonly object _audioLock = new object(); // Synchronize audio device access
        private readonly object _pipeLock = new object(); // Synchronize pipe writes
        private readonly object _lifecycleLock = new object();
        private ConcurrentDictionary<uint, DateTime> _lastKeyPressTime = new(); // Track last key press for debouncing - thread-safe
        private Task? _workerTask;
        private Task? _monitorTask;
        private long _lastHeartbeatTicks = DateTime.UtcNow.Ticks;
        private bool _restartInProgress;

        public LenoPwnService()
        {
            this.ServiceName = "LenoPwn.Service";
            this.CanStop = true;
            this.CanPauseAndContinue = false;
            this.CanHandlePowerEvent = true;
            this.AutoLog = false;
        }

        private void Log(string message, EventLogEntryType type = EventLogEntryType.Information)
        {
            try { EventLog.WriteEntry(ServiceName, message, type); } catch { }
        }

        public static void Main() => ServiceBase.Run(new LenoPwnService());

        protected override void OnStart(string[] args)
        {
            StartWorker();
        }

        protected override void OnStop()
        {
            StopWorkerAsync().GetAwaiter().GetResult();
            CleanupRuntimeState();
        }

        protected override bool OnPowerEvent(PowerBroadcastStatus powerStatus)
        {
            if (powerStatus == PowerBroadcastStatus.ResumeSuspend)
            {
                Log("Resume detected. Restarting service worker to refresh WMI, audio, and pipe state.");
                Task.Run(RestartAfterResumeAsync).ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        Log($"Resume restart failed: {t.Exception?.GetBaseException().Message}", EventLogEntryType.Error);
                    }
                }, TaskContinuationOptions.ExecuteSynchronously);
                return true;
            }

            return base.OnPowerEvent(powerStatus);
        }

        private void StartWorker()
        {
            lock (_lifecycleLock)
            {
                _cancellationTokenSource = new CancellationTokenSource();
                _workerTask = Task.Run(() => ServiceWorker(_cancellationTokenSource.Token));
                _monitorTask = Task.Run(() => MonitorAndRestoreAsync(_cancellationTokenSource.Token));
            }
        }

        private async Task StopWorkerAsync()
        {
            Task? workerTask;
            Task? monitorTask;
            CancellationTokenSource? cts;

            lock (_lifecycleLock)
            {
                workerTask = _workerTask;
                monitorTask = _monitorTask;
                cts = _cancellationTokenSource;
                _workerTask = null;
                _monitorTask = null;
                _cancellationTokenSource = null;
            }

            cts?.Cancel();

            var tasks = new List<Task?> { workerTask, monitorTask }.Where(t => t != null).Cast<Task>().ToArray();

            if (tasks.Length > 0)
            {
                try
                {
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    Log($"Service worker/monitor stopped with error: {ex.Message}", EventLogEntryType.Warning);
                }
            }
        }

        private void CleanupRuntimeState()
        {
            lock (_lifecycleLock)
            {
                _lastKeyPressTime.Clear();
            }

            _watcher?.Stop();
            _watcher?.Dispose();
            _watcher = null;

            _pipeWriter?.Dispose();
            _pipeWriter = null;

            _pipeClient?.Dispose();
            _pipeClient = null;

            _configWatcher?.Dispose();
            _configWatcher = null;

            CleanupAudioMonitors();
        }

        private async Task RestartAfterResumeAsync()
        {
            lock (_lifecycleLock)
            {
                if (_restartInProgress)
                {
                    return;
                }

                _restartInProgress = true;
            }

            try
            {
                await StopWorkerAsync().ConfigureAwait(false);
                CleanupRuntimeState();
                StartWorker();
            }
            finally
            {
                lock (_lifecycleLock)
                {
                    _restartInProgress = false;
                }
            }
        }

        private async Task MonitorAndRestoreAsync(CancellationToken token)
        {
            const int monitorIntervalMs = 30000;
            var threshold = TimeSpan.FromMinutes(5);

            while (!token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(monitorIntervalMs, token);
                    if (token.IsCancellationRequested) break;

                    Task? currentWorker;
                    lock (_lifecycleLock)
                    {
                        currentWorker = _workerTask;
                    }

                    if (currentWorker == null || currentWorker.IsCompleted)
                    {
                        Log("Service worker task is not running. Attempting restart.", EventLogEntryType.Warning);
                        await RestartAfterResumeAsync().ConfigureAwait(false);
                        continue;
                    }

                    var elapsed = TimeSpan.FromTicks(DateTime.UtcNow.Ticks - Interlocked.Read(ref _lastHeartbeatTicks));
                    if (elapsed > threshold)
                    {
                        Log($"Detected possible extended sleep/standby: heartbeat stale ({elapsed}). Restarting worker.", EventLogEntryType.Warning);
                        await RestartAfterResumeAsync().ConfigureAwait(false);
                        continue;
                    }

                    if (_watcher == null)
                    {
                        Log("WMI watcher is null. Attempting restart.", EventLogEntryType.Warning);
                        await RestartAfterResumeAsync().ConfigureAwait(false);
                        continue;
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    Log($"Monitor encountered exception: {ex.Message}", EventLogEntryType.Warning);
                }
            }
        }

        private async Task ServiceWorker(CancellationToken token)
        {
            try
            {
                InitializeAudioMonitors();
                WmiController.SyncMicMuteLed();
                WmiController.SyncSpeakerMuteLed();

                await PollForInitialConfigurationAsync(token);

                if (token.IsCancellationRequested) return;

                _watcher = new ManagementEventWatcher(@"\\.\root\WMI", "SELECT * FROM LENOVO_UTILITY_EVENT");
                _watcher.EventArrived += OnEventArrived;
                _watcher.Start();
                Log("WMI watcher started. Service is now listening for key presses.");

                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        Interlocked.Exchange(ref _lastHeartbeatTicks, DateTime.UtcNow.Ticks);
                        if (_pipeClient == null || !_pipeClient.IsConnected)
                        {
                            ResetPipeConnection();
                            _pipeClient = new NamedPipeClientStream(".", PipeName, PipeDirection.Out, PipeOptions.Asynchronous);
                            Log("Attempting to connect to agent pipe...");

                            await _pipeClient.ConnectAsync(5000, token);
                            _pipeClient.WriteTimeout = 1000;
                            _pipeWriter = new StreamWriter(_pipeClient, Encoding.UTF8) { AutoFlush = true };
                            Log("Successfully connected to agent pipe!");
                        }

                        await Task.Delay(1000, token);
                    }
                    catch (OperationCanceledException)
                    {
                        Log("Service worker cancellation requested.");
                        break;
                    }
                    catch (System.TimeoutException)
                    {
                        Log("Timeout connecting to agent pipe. Will retry in 5 seconds...", EventLogEntryType.Warning);
                        ResetPipeConnection();
                        await Task.Delay(5000, token);
                    }
                    catch (Exception ex)
                    {
                        Log($"Pipe client error: {ex.Message}. Will retry in 5 seconds...", EventLogEntryType.Warning);
                        ResetPipeConnection();
                        await Task.Delay(5000, token);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Log("Service worker was cancelled during startup.");
            }
            catch (Exception ex)
            {
                Log($"An unhandled exception occurred in the service worker: {ex.Message}", EventLogEntryType.Error);
            }

            Log("Service worker loop ended.");
        }

        private void OnEventArrived(object sender, EventArrivedEventArgs e)
        {
            if (e.NewEvent.GetPropertyValue("PressTypeDataVal") is not uint keyCode) return;

            // Debounce rapid key presses
            var now = DateTime.UtcNow;
            if (_lastKeyPressTime.TryGetValue(keyCode, out var lastTime))
            {
                if ((now - lastTime).TotalMilliseconds < DebouncingDelayMs)
                {
                    return; // Ignore debounced event
                }
            }
            _lastKeyPressTime.AddOrUpdate(keyCode, now, (k, v) => now);

            AppConfig? config;
            lock (_configLock)
            {
                config = _cachedConfig;
            }

            if (config == null)
            {
                Log("Configuration not loaded, skipping execution.", EventLogEntryType.Warning);
                return;
            }

            _currentTheme = string.IsNullOrWhiteSpace(config.Theme) ? "Dark" : config.Theme;
            var mapping = config.Mappings.FirstOrDefault(m => m.KeyCode == keyCode);
            if (mapping != null) ExecuteAction(mapping);
        }

        private void ExecuteAction(HotkeyMapping mapping)
        {
            string? payloadStr = (mapping.Payload as JsonElement?)?.GetString() ?? mapping.Payload as string;
            switch (mapping.Action.ToLower())
            {
                case "launch":
                    if (string.IsNullOrEmpty(payloadStr)) return;
                    SendCommandToAgent($"launch::{payloadStr}");
                    break;

                case "sendkeys":
                    if (mapping.Payload is SendKeysPayload sendKeysPayload)
                    {
                        string key = sendKeysPayload.Key ?? "";
                        string modifiersStr = string.Join(",", sendKeysPayload.Modifiers ?? new List<string>());

                        if (!string.IsNullOrEmpty(key))
                        {
                            SendCommandToAgent($"sendkeys::{modifiersStr}::{key}");
                        }
                    }
                    else if (mapping.Payload is JsonElement jsonPayload)
                    {
                        try
                        {
                            if (jsonPayload.TryGetProperty("Key", out var keyProp) && keyProp.ValueKind == JsonValueKind.String)
                            {
                                string key = keyProp.GetString() ?? "";
                                string modifiersStr = "";

                                if (jsonPayload.TryGetProperty("Modifiers", out var modProp) && modProp.ValueKind == JsonValueKind.Array)
                                {
                                    var modifiers = modProp.EnumerateArray().Select(m => m.GetString() ?? "").Where(s => !string.IsNullOrEmpty(s)).ToList();
                                    modifiersStr = string.Join(",", modifiers);
                                }

                                if (!string.IsNullOrEmpty(key))
                                {
                                    SendCommandToAgent($"sendkeys::{modifiersStr}::{key}");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Log($"Failed to execute sendkeys action: {ex.Message}", EventLogEntryType.Error);
                        }
                    }
                    break;

                case "special":
                    if (string.IsNullOrEmpty(payloadStr)) return;
                    string iconName = payloadStr;
                    if (payloadStr == "toggle_mic_mute")
                    {
                        lock (_audioLock)
                        {
                            ToggleCoreAudioMuteAndLed();
                            iconName = (_micDevice?.AudioEndpointVolume.Mute ?? false) ? "microphone_mute" : "microphone_unmute";
                        }
                    }
                    else if (payloadStr == "toggle_speaker_mute")
                    {
                        lock (_audioLock)
                        {
                            ToggleCoreAudioSpeakerMuteAndLed();
                            iconName = (_speakerDevice?.AudioEndpointVolume.Mute ?? false) ? "speaker_mute" : "speaker_unmute";
                        }
                    }
                    if (mapping.ShowPopup) SendCommandToAgent($"show_icon::{iconName}::{_currentTheme}");
                    break;
            }
        }

        private void ShowPopup(string iconName) => SendCommandToAgent($"show_icon::{iconName}::{_currentTheme}");

        private void SendCommandToAgent(string command)
        {
            lock (_pipeLock)
            {
                if (_pipeClient?.IsConnected == true && _pipeWriter != null)
                {
                    try
                    {
                        _pipeWriter.WriteLine(command);
                    }
                    catch (IOException ex)
                    {
                        Log($"Pipe write error: {ex.Message}", EventLogEntryType.Warning);
                        ResetPipeConnection();
                    }
                    catch (System.TimeoutException ex)
                    {
                        Log($"Pipe write timeout: {ex.Message}", EventLogEntryType.Warning);
                        ResetPipeConnection();
                    }
                }
            }
        }

        private void ResetPipeConnection()
        {
            lock (_pipeLock)
            {
                _pipeWriter?.Dispose();
                _pipeWriter = null;

                _pipeClient?.Dispose();
                _pipeClient = null;
            }
        }

        #region User Config and Audio
        private async Task PollForInitialConfigurationAsync(CancellationToken token)
        {
            const int retryDelayMs = 10000; // 10 seconds

            while (!token.IsCancellationRequested)
            {
                string? configPath = GetConfigPathForActiveUser();

                if (!string.IsNullOrEmpty(configPath) && LoadConfiguration(configPath))
                {
                    Log("Initial configuration loaded successfully.");
                    InitializeConfigWatcher(configPath);
                    return;
                }

                Log($"Configuration not found or failed to load. Retrying in {retryDelayMs / 1000} seconds...", EventLogEntryType.Warning);
                await Task.Delay(retryDelayMs, token);
            }
        }

        private void InitializeConfigWatcher(string configPath)
        {
            var configDirectory = Path.GetDirectoryName(configPath);
            if (configDirectory != null)
            {
                _configWatcher = new FileSystemWatcher(configDirectory, ConfigFileName)
                {
                    NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName
                };
                _configWatcher.Changed += (s, e) => LoadConfiguration(e.FullPath);
                _configWatcher.Created += (s, e) => LoadConfiguration(e.FullPath);
                _configWatcher.Deleted += (s, e) =>
                {
                    lock (_configLock)
                    {
                        _cachedConfig = null;
                    }
                    Log("Config file deleted. Cache cleared. Service will attempt to reload.");
                };
                _configWatcher.EnableRaisingEvents = true;
                Log($"Watching for config changes at: {configPath}");
            }
        }

        private bool LoadConfiguration(string configPath)
        {
            try
            {
                if (!File.Exists(configPath))
                {
                    return false;
                }

                var json = File.ReadAllText(configPath);
                var config = JsonSerializer.Deserialize<AppConfig>(json) ?? new AppConfig();

                foreach (var mapping in config.Mappings)
                {
                    if (mapping.Action?.ToLower() == "sendkeys" && mapping.Payload is JsonElement element)
                    {
                        try
                        {
                            mapping.Payload = JsonSerializer.Deserialize<SendKeysPayload>(element.GetRawText());
                        }
                        catch
                        {
                            mapping.Payload = new SendKeysPayload();
                        }
                    }
                }

                lock (_configLock)
                {
                    _cachedConfig = config;
                }
                Log("Configuration reloaded and cached successfully.");
                return true;
            }
            catch (Exception ex)
            {
                Log($"Failed to load or parse configuration: {ex.Message}", EventLogEntryType.Error);
                lock (_configLock)
                {
                    _cachedConfig = null;
                }
                return false;
            }
        }

        private string? GetConfigPathForActiveUser()
        {
            uint sessionId = WTSGetActiveConsoleSessionId();
            if (sessionId == 0xFFFFFFFF) return null;
            IntPtr userToken = IntPtr.Zero;
            try
            {
                if (!WTSQueryUserToken(sessionId, out userToken)) return null;
                uint size = 260;
                var profileDir = new StringBuilder((int)size);
                if (!GetUserProfileDirectory(userToken, profileDir, ref size)) return null;
                var configFolder = Path.Combine(profileDir.ToString(), "AppData", "Local", "LenoPwn");
                return Path.Combine(configFolder, ConfigFileName);
            }
            catch { return null; }
            finally { if (userToken != IntPtr.Zero) CloseHandle(userToken); }
        }

        private void InitializeAudioMonitors()
        {
            try
            {
                CleanupAudioMonitors();

                lock (_audioLock)
                {
                    _deviceEnumerator = new MMDeviceEnumerator();
                    _micDevice = _deviceEnumerator.GetDefaultAudioEndpoint(DataFlow.Capture, Role.Communications);
                    _speakerDevice = _deviceEnumerator.GetDefaultAudioEndpoint(DataFlow.Render, Role.Console);

                    Log("Audio monitors initialized successfully.");
                }
            }
            catch (Exception ex)
            {
                Log($"Could not initialize audio monitors: {ex.Message}.", EventLogEntryType.Warning);
            }
        }

        private void CleanupAudioMonitors()
        {
            lock (_audioLock)
            {
                _micDevice?.Dispose();
                _speakerDevice?.Dispose();
                _deviceEnumerator?.Dispose();

                _micDevice = null;
                _speakerDevice = null;
                _deviceEnumerator = null;
            }
        }

        private void ToggleCoreAudioMuteAndLed() 
        { 
            // Note: This is called within _audioLock from ExecuteAction
            try
            {
                using var deviceEnumerator = new MMDeviceEnumerator();
                using var micDevice = deviceEnumerator.GetDefaultAudioEndpoint(DataFlow.Capture, Role.Communications);

                if (micDevice == null)
                {
                    Log("Failed to toggle microphone mute: no capture device is available.", EventLogEntryType.Warning);
                    return;
                }

                var newMuteState = !micDevice.AudioEndpointVolume.Mute;
                micDevice.AudioEndpointVolume.Mute = newMuteState;
                WmiController.SetMicMuteLedState(newMuteState);
            }
            catch (Exception ex)
            {
                Log($"Failed to toggle microphone mute: {ex.Message}", EventLogEntryType.Error);
            }
        }

        private void ToggleCoreAudioSpeakerMuteAndLed() 
        { 
            // Note: This is called within _audioLock from ExecuteAction
            try
            {
                using var deviceEnumerator = new MMDeviceEnumerator();
                using var speakerDevice = deviceEnumerator.GetDefaultAudioEndpoint(DataFlow.Render, Role.Console);

                if (speakerDevice == null)
                {
                    Log("Failed to toggle speaker mute: no render device is available.", EventLogEntryType.Warning);
                    return;
                }

                var newMuteState = !speakerDevice.AudioEndpointVolume.Mute;
                speakerDevice.AudioEndpointVolume.Mute = newMuteState;
                WmiController.SetSpeakerMuteLedState(newMuteState);
            }
            catch (Exception ex)
            {
                Log($"Failed to toggle speaker mute: {ex.Message}", EventLogEntryType.Error);
            }
        }
        #endregion

        #region P/Invoke
        [DllImport("userenv.dll", SetLastError = true, CharSet = CharSet.Auto)]
        private static extern bool GetUserProfileDirectory(IntPtr hToken, StringBuilder lpProfileDir, ref uint lpcchSize);

        [DllImport("wtsapi32.dll", SetLastError = true)]
        private static extern bool WTSQueryUserToken(uint SessionId, out IntPtr phToken);

        [DllImport("kernel32.dll")]
        private static extern uint WTSGetActiveConsoleSessionId();

        [DllImport("kernel32.dll")]
        private static extern bool CloseHandle(IntPtr handle);
        #endregion
    }

    #region Helper Classes
    public class AppConfig { public string Theme { get; set; } = "Dark"; public List<HotkeyMapping> Mappings { get; set; } = new(); }
    public class HotkeyMapping { public uint KeyCode { get; set; } public string Description { get; set; } = ""; public string Action { get; set; } = "Not Assigned"; public object? Payload { get; set; } public bool ShowPopup { get; set; } = false; }

    public class SendKeysPayload
    {
        public List<string> Modifiers { get; set; } = new();
        public string Key { get; set; } = "";
    }

    public class HotkeyMappingPayloadConverter : JsonConverter<object> { public override object? Read(ref Utf8JsonReader r, Type t, JsonSerializerOptions o) => r.TokenType switch { JsonTokenType.String => r.GetString(), _ => JsonDocument.ParseValue(ref r).RootElement.Clone(), }; public override void Write(Utf8JsonWriter w, object v, JsonSerializerOptions o) => JsonSerializer.Serialize(w, v, v.GetType(), o); }

    [SupportedOSPlatform("windows")]
    public static class WmiController
    {
        private const uint MIC_MUTE_LED_ON = 1, MIC_MUTE_LED_OFF = 2, SPEAKER_MUTE_LED_ON = 4, SPEAKER_MUTE_LED_OFF = 5;
        private static readonly object _ledLock = new object();

        private static void SetLedFeature(uint fc)
        {
            lock (_ledLock)
            {
                try
                {
                    using var mc = new ManagementClass(@"\\.\root\WMI", "LENOVO_UTILITY_DATA", null);
                    using var mi = mc.GetInstances().Cast<ManagementObject>().FirstOrDefault();
                    if (mi == null) return;
                    var ip = mc.GetMethodParameters("SetFeature");
                    ip["featuretype"] = fc;
                    mi.InvokeMethod("SetFeature", ip, null);
                }
                catch
                {
                    // Silent fail - WMI might not be available on all systems
                }
            }
        }

        public static void SetMicMuteLedState(bool isMuted) => SetLedFeature(isMuted ? MIC_MUTE_LED_ON : MIC_MUTE_LED_OFF);
        public static void SetSpeakerMuteLedState(bool isMuted) => SetLedFeature(isMuted ? SPEAKER_MUTE_LED_ON : SPEAKER_MUTE_LED_OFF);

        public static void SyncMicMuteLed()
        {
            try
            {
                using var e = new MMDeviceEnumerator();
                using var m = e.GetDefaultAudioEndpoint(DataFlow.Capture, Role.Communications);
                if (m != null) SetMicMuteLedState(m.AudioEndpointVolume.Mute);
            }
            catch
            {
                // Silent fail
            }
        }

        public static void SyncSpeakerMuteLed()
        {
            try
            {
                using var e = new MMDeviceEnumerator();
                using var s = e.GetDefaultAudioEndpoint(DataFlow.Render, Role.Console);
                if (s != null) SetSpeakerMuteLedState(s.AudioEndpointVolume.Mute);
            }
            catch
            {
                // Silent fail
            }
        }
    }

    public static class CancellationTokenExtensions
    {
        public static Task AsTask(this CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<object>();
            cancellationToken.Register(() => tcs.TrySetCanceled(), useSynchronizationContext: false);
            return tcs.Task;
        }
    }
    #endregion
}
