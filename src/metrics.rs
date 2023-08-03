use usiem::prelude::{
    counter::{Counter, CounterVec},
    gauge::{Gauge, GaugeVec},
    metrics::*,
};

pub fn generate_kernel_metrics() -> (Vec<SiemMetricDefinition>, KernelMetrics) {
    let queued_logs_parsing = SiemMetricDefinition::new(
        "queued_logs_parsing",
        "Number of logs in the parsing queue",
        SiemMetric::Gauge(GaugeVec::new(&[&[]])),
    )
    .unwrap();
    let queued_logs_enrichment = SiemMetricDefinition::new(
        "queued_logs_enrichment",
        "Number of logs in the enrichment queue",
        SiemMetric::Gauge(GaugeVec::new(&[&[]])),
    )
    .unwrap();
    let queued_logs_rule_engine = SiemMetricDefinition::new(
        "queued_logs_rule_engine",
        "Number of logs in the rule engine queue",
        SiemMetric::Gauge(GaugeVec::new(&[&[]])),
    )
    .unwrap();
    let queued_logs_indexing = SiemMetricDefinition::new(
        "queued_logs_indexing",
        "Number of logs in the indexing queue",
        SiemMetric::Gauge(GaugeVec::new(&[&[]])),
    )
    .unwrap();
    let queued_messages_for_kernel = SiemMetricDefinition::new(
        "queued_messages_for_kernel",
        "Number of messages that the kernel has in the queue",
        SiemMetric::Gauge(GaugeVec::new(&[&[]])),
    )
    .unwrap();
    let total_messages_processed_by_kernel = SiemMetricDefinition::new(
        "total_messages_processed_by_kernel",
        "Total number of messages processed by the kernel",
        SiemMetric::Counter(CounterVec::new(&[&[]])),
    )
    .unwrap();
    let metrics = KernelMetrics {
        queued_logs_parsing: get_metric(&queued_logs_parsing),
        queued_logs_enrichment: get_metric(&queued_logs_enrichment),
        queued_logs_rule_engine: get_metric(&queued_logs_rule_engine),
        queued_logs_indexing: get_metric(&queued_logs_indexing),
        queued_messages_for_kernel: get_metric(&queued_messages_for_kernel),
        total_messages_processed_by_kernel: get_metric_counter(&total_messages_processed_by_kernel),
    };
    (vec![queued_logs_parsing, queued_logs_enrichment, queued_logs_rule_engine, queued_logs_indexing, queued_messages_for_kernel, total_messages_processed_by_kernel], metrics)
}

fn get_metric(definition: &SiemMetricDefinition) -> Gauge {
    let gauge_vec: GaugeVec = definition.metric().try_into().unwrap();
    gauge_vec.with_labels(&[]).unwrap().clone()
}
fn get_metric_counter(definition: &SiemMetricDefinition) -> Counter {
    let vc: CounterVec = definition.metric().try_into().unwrap();
    vc.with_labels(&[]).unwrap().clone()
}

#[derive(Clone)]
pub struct KernelMetrics {
    pub queued_logs_parsing: Gauge,
    pub queued_logs_enrichment: Gauge,
    pub queued_logs_rule_engine: Gauge,
    pub queued_logs_indexing: Gauge,
    pub queued_messages_for_kernel: Gauge,
    pub total_messages_processed_by_kernel: Counter,
}
