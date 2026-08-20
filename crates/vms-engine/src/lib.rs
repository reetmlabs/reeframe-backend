// vms-engine — event bus, pipeline registry, trigger evaluator, scheduler,
//              pipeline executor, resource manager, and stat monitor.

pub mod coverage;
pub mod event_bus;
pub mod metrics;
pub mod pipeline_executor;
pub mod pipeline_registry;
pub mod resource_manager;
pub mod stat_monitor;
pub mod time_helpers;
pub mod trigger_evaluator;

pub use event_bus::{EventBus, DEFAULT_CAPACITY};
pub use metrics::Metrics;
pub use pipeline_executor::PipelineExecutor;
pub use pipeline_registry::PipelineRegistry;
pub use resource_manager::ResourceManager;
pub use stat_monitor::{RetentionConfig, StatMonitor};
pub use trigger_evaluator::TriggerEvaluator;
