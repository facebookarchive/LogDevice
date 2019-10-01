namespace py3 logdevice.configuration
namespace cpp2 facebook.logdevice.configuration.all_read_streams_debug_config.thrift

struct AllReadStreamsDebugConfig {
  # Emit debug line with such Client Session ID
  1: string csid;

  # Epoch unix timestamp in seconds
  2: i64 deadline;

  # Reason of having this change to configs
  3: string context;
}

struct AllReadStreamsDebugConfigs {
  # List of all the active debug configs.
  1: list<AllReadStreamsDebugConfig> configs;
}
