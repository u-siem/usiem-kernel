use usiem::{
    crossbeam_channel::{self, Receiver, Sender},
    prelude::{SiemAlert, SiemLog, SiemMessage},
};

use crate::metrics::*;

#[derive(Clone)]
pub struct ComponentChannels {
    pub alert_channel: (Receiver<SiemAlert>, Sender<SiemAlert>),
    pub kernel_channel: (Receiver<SiemMessage>, Sender<SiemMessage>),
    pub parser_channel: (Receiver<SiemLog>, Sender<SiemLog>),
    pub enricher_channel: (Receiver<SiemLog>, Sender<SiemLog>),
    pub rule_engine_channel: (Receiver<SiemLog>, Sender<SiemLog>),
    pub output_channel: (Receiver<SiemLog>, Sender<SiemLog>),
    pub wal_log: (Receiver<SiemLog>, Sender<SiemLog>),
    pub scaling_limit: f64,
    pub metrics: KernelMetrics,
}

impl ComponentChannels {
    pub fn new(channel_size: usize, scaling_limit: f64, metrics: KernelMetrics) -> Self {
        let (os, or) = crossbeam_channel::bounded(channel_size);
        let (ps, pr) = crossbeam_channel::bounded(channel_size);
        let (es, er) = crossbeam_channel::bounded(channel_size);
        let (rs, rr) = crossbeam_channel::bounded(channel_size);
        let (is, ir) = crossbeam_channel::bounded(channel_size);
        let (ws, wr) = crossbeam_channel::bounded(channel_size);
        let (alert_s, alert_r) = crossbeam_channel::bounded(channel_size);

        Self {
            kernel_channel: (or, os),
            parser_channel: (pr, ps),
            enricher_channel: (er, es),
            rule_engine_channel: (rr, rs),
            output_channel: (ir, is),
            wal_log: (wr, ws),
            alert_channel: (alert_r, alert_s),
            scaling_limit,
            metrics,
        }
    }

    pub fn update_metrics(&self) {
        self.metrics
            .queued_logs_parsing
            .set(self.parser_channel.0.len() as f64);
        self.metrics
            .queued_logs_enrichment
            .set(self.enricher_channel.0.len() as f64);
        self.metrics
            .queued_logs_rule_engine
            .set(self.rule_engine_channel.0.len() as f64);
        self.metrics
            .queued_logs_indexing
            .set(self.output_channel.0.len() as f64);
        self.metrics
            .queued_messages_for_kernel
            .set(self.parser_channel.0.len() as f64);
        self.metrics
            .queued_logs_parsing
            .set(self.kernel_channel.0.len() as f64);
    }

    pub fn scale_parser(&self) -> ScaleAction {
        Self::channel_needs_to_scale_up(&self.parser_channel.0)
    }
    pub fn scale_enricher(&self) -> ScaleAction {
        Self::channel_needs_to_scale_up(&self.enricher_channel.0)
    }
    pub fn scale_rules(&self) -> ScaleAction {
        Self::channel_needs_to_scale_up(&self.rule_engine_channel.0)
    }
    pub fn scale_output(&self) -> ScaleAction {
        Self::channel_needs_to_scale_up(&self.output_channel.0)
    }

    fn channel_needs_to_scale_up<T>(channel: &Receiver<T>) -> ScaleAction {
        let messages = channel.len();
        let capacity = channel.capacity().unwrap_or(0);
        if (messages as f64) > (0.9 * (capacity as f64)) {
            ScaleAction::ScaleUp
        } else {
            ScaleAction::Skip
        }
    }
}

pub enum ScaleAction {
    ScaleUp,
    //ScaleDown,
    Skip,
}
