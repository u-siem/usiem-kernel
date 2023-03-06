use std::{collections::BTreeMap, sync::{atomic::AtomicI64, Arc}};

use usiem::prelude::{ metrics::*, types::*};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref QUEUED_LOGS_PARSING : Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
    pub static ref QUEUED_LOGS_ENRICHMENT : Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
    pub static ref QUEUED_LOGS_RULE_ENGINE : Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
    pub static ref QUEUED_LOGS_INDEXING : Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
    pub static ref QUEUED_MESSAGES_FOR_KERNEL : Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
    pub static ref TOTAL_MESSAGES_PROCESSED : Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
}

pub fn generate_kernel_metrics() -> Vec<SiemMetricDefinition> {
    vec![
        SiemMetricDefinition {
            metric: SiemMetric::Gauge(QUEUED_LOGS_PARSING.clone(), 1.0),
            name: LogString::Borrowed("queued_logs_parsing"),
            description: LogString::Borrowed("Number of logs in the parsing queue"),
            labels: BTreeMap::new(),
        },
        SiemMetricDefinition {
            metric: SiemMetric::Gauge(QUEUED_LOGS_ENRICHMENT.clone(), 1.0),
            name: LogString::Borrowed("queued_logs_enrichment"),
            description: LogString::Borrowed("Number of logs in the enrichment queue"),
            labels: BTreeMap::new(),
        },
        SiemMetricDefinition {
            metric: SiemMetric::Gauge(QUEUED_LOGS_RULE_ENGINE.clone(), 1.0),
            name: LogString::Borrowed("queued_logs_rule_engine"),
            description: LogString::Borrowed("Number of logs in the rule engine queue"),
            labels: BTreeMap::new(),
        },
        SiemMetricDefinition {
            metric: SiemMetric::Gauge(QUEUED_LOGS_INDEXING.clone(), 1.0),
            name: LogString::Borrowed("queued_logs_indexing"),
            description: LogString::Borrowed("Number of logs in the indexing queue"),
            labels: BTreeMap::new(),
        },
        SiemMetricDefinition {
            metric: SiemMetric::Gauge(QUEUED_MESSAGES_FOR_KERNEL.clone(), 1.0),
            name: LogString::Borrowed("queued_messages_for_kernel"),
            description: LogString::Borrowed("Number of messages that the kernel has in the queue"),
            labels: BTreeMap::new(),
        },
        SiemMetricDefinition {
            metric: SiemMetric::Counter(TOTAL_MESSAGES_PROCESSED.clone()),
            name: LogString::Borrowed("total_messages_processed_by_kernel"),
            description: LogString::Borrowed("Total number of messages processed by the kernel"),
            labels: BTreeMap::new(),
        },
    ]
}
