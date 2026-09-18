// vms-engine — event bus, pipeline registry, trigger evaluator, scheduler,
//              pipeline executor, resource manager, and stat monitor.

pub mod chunk_reconciliation;
pub mod coverage;
pub mod event_bus;
pub mod metrics;
pub mod pipeline_executor;
pub mod pipeline_registry;
pub mod recording_intent;
pub mod resource_manager;
pub mod stat_monitor;
pub mod time_helpers;
pub mod trigger_evaluator;

pub use chunk_reconciliation::reconcile_orphaned_chunks;
pub use event_bus::{EventBus, DEFAULT_CAPACITY};
pub use metrics::Metrics;
pub use pipeline_executor::PipelineExecutor;
pub use pipeline_registry::PipelineRegistry;
pub use recording_intent::reconcile_recording_intent;
pub use resource_manager::ResourceManager;
pub use stat_monitor::{CoverageConfig, RecordingIntentConfig, RetentionConfig, StatMonitor};
pub use trigger_evaluator::TriggerEvaluator;
pub use vms_transports::TransportDispatcher;
