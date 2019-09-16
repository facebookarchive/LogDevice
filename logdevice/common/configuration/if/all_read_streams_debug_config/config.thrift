namespace py3 logdevice.all_read_streams_debug_config
namespace cpp2 logdevice.all_read_streams_debug_config.config

struct LogdeviceAllReadStreamsDebugConfig {
  1: set<string> allowed_csids = [];
  2: optional i64 deadline;
}
