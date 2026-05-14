// vms-engine — event bus, pipeline registry, trigger evaluator, scheduler,
//              pipeline executor, resource manager, and stat monitor.

pub mod event_bus;
pub mod pipeline_registry;

pub use event_bus::{EventBus, DEFAULT_CAPACITY};
pub use pipeline_registry::PipelineRegistry;
