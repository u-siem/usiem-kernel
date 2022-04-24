use usiem::components::dataset::holder::DatasetHolder;
use usiem::crossbeam_channel::{Receiver, Sender, TryRecvError};
use usiem::crossbeam_channel;
use usiem::chrono;
use lazy_static::lazy_static;
use std::sync::atomic::AtomicI64;
use usiem::components::metrics::{SiemMetricDefinition, SiemMetric};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use usiem::components::common::{
    SiemComponentStateStorage, SiemMessage,
};
use usiem::components::command::{SiemCommandCall, SiemCommandHeader};
use usiem::components::dataset::{SiemDataset, SiemDatasetType};
use usiem::components::{SiemComponent, SiemDatasetManager};
use usiem::events::SiemLog;

#[cfg(test)]
mod test_comp;


lazy_static! {
    pub static ref QUEUED_LOGS_PARSING : Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
    pub static ref QUEUED_LOGS_ENRICHMENT : Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
    pub static ref QUEUED_LOGS_RULE_ENGINE : Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
    pub static ref QUEUED_LOGS_INDEXING : Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
    pub static ref QUEUED_MESSAGES_FOR_KERNEL : Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
    pub static ref TOTAL_MESSAGES_PROCESSED : Arc<AtomicI64> = Arc::new(AtomicI64::new(0));
}

pub struct SiemBasicKernel {
    own_channel: (Receiver<SiemMessage>, Sender<SiemMessage>),
    // Channels to be asigned to specific components
    parser_channel: (Receiver<SiemLog>, Sender<SiemLog>),
    enricher_channel: (Receiver<SiemLog>, Sender<SiemLog>),
    rule_engine_channel: (Receiver<SiemLog>, Sender<SiemLog>),
    output_channel: (Receiver<SiemLog>, Sender<SiemLog>),
    /// Components that receive logs
    input_components: Vec<Box<dyn SiemComponent>>,
    /// Other components not used in the log processing pipeline
    other_components: Vec<Box<dyn SiemComponent>>,
    /// Components that does not need to be run
    norun_components: Vec<Box<dyn SiemComponent>>,
    /// Component that parses logs
    parser_component: Option<Box<dyn SiemComponent>>,
    /// Component that applies rules to the logs
    rule_engine_component: Option<Box<dyn SiemComponent>>,
    /// Component that enriches logs with information extracted from datasets or updates the datasets
    enricher_component: Option<Box<dyn SiemComponent>>,
    /// Component that stores logs or querys them
    output_component: Option<Box<dyn SiemComponent>>,
    /// Component responsible of managing the datasets
    dataset_manager: Option<Box<dyn SiemDatasetManager>>,
    /// Component that allows to store or retrieve information
    state_storage: Option<Box<dyn SiemComponentStateStorage>>,
    /// Alerting component
    alert_component: Option<Box<dyn SiemComponent>>,
    metrics : Vec<SiemMetricDefinition>,
    pub max_threads_parsing: u64,
    pub max_threads_enchancing: u64,
    pub max_threads_output: u64,
    pub max_threads_rule_engine: u64,
    pub command_response_timeout: i64,
    command_map: BTreeMap<String, Vec<String>>,
    dataset_map: BTreeMap<SiemDatasetType, Vec<String>>,
    task_map: BTreeMap<String, Vec<String>>,
    dataset_channels: Arc<Mutex<BTreeMap<SiemDatasetType, Vec<Sender<SiemMessage>>>>>,
}

impl SiemBasicKernel {
    pub fn new(queue_size: usize, max_threads: u64, command_timeout: i64) -> SiemBasicKernel {
        let (os, or) = crossbeam_channel::bounded(queue_size);
        let (ps, pr) = crossbeam_channel::bounded(queue_size);
        let (es, er) = crossbeam_channel::bounded(queue_size);
        let (rs, rr) = crossbeam_channel::bounded(queue_size);
        let (is, ir) = crossbeam_channel::bounded(queue_size);
        
        let mut metrics = Vec::new();
        metrics.push(SiemMetricDefinition {
            metric : SiemMetric::Gauge(QUEUED_LOGS_PARSING.clone(), 1.0),
            name : Cow::Borrowed("queued_logs_parsing"),
            description : Cow::Borrowed("Number of logs in the parsing queue"),
            tags : BTreeMap::new()
        });
        metrics.push(SiemMetricDefinition {
            metric : SiemMetric::Gauge(QUEUED_LOGS_ENRICHMENT.clone(), 1.0),
            name : Cow::Borrowed("queued_logs_enrichment"),
            description : Cow::Borrowed("Number of logs in the enrichment queue"),
            tags : BTreeMap::new()
        });
        metrics.push(SiemMetricDefinition {
            metric : SiemMetric::Gauge(QUEUED_LOGS_RULE_ENGINE.clone(), 1.0),
            name : Cow::Borrowed("queued_logs_rule_engine"),
            description : Cow::Borrowed("Number of logs in the rule engine queue"),
            tags : BTreeMap::new()
        });
        metrics.push(SiemMetricDefinition {
            metric : SiemMetric::Gauge(QUEUED_LOGS_INDEXING.clone(), 1.0),
            name : Cow::Borrowed("queued_logs_indexing"),
            description : Cow::Borrowed("Number of logs in the indexing queue"),
            tags : BTreeMap::new()
        });
        metrics.push(SiemMetricDefinition {
            metric : SiemMetric::Gauge(QUEUED_MESSAGES_FOR_KERNEL.clone(), 1.0),
            name : Cow::Borrowed("queued_messages_for_kernel"),
            description : Cow::Borrowed("Number of messages that the kernel has in the queue"),
            tags : BTreeMap::new()
        });
        metrics.push(SiemMetricDefinition {
            metric : SiemMetric::Counter(TOTAL_MESSAGES_PROCESSED.clone()),
            name : Cow::Borrowed("total_messages_processed"),
            description : Cow::Borrowed("Total number of messages processed by the kernel"),
            tags : BTreeMap::new()
        });

        return SiemBasicKernel {
            own_channel: (or, os),
            parser_channel: (pr, ps),
            enricher_channel: (er, es),
            rule_engine_channel: (rr, rs),
            output_channel: (ir, is),
            input_components: Vec::new(),
            other_components: Vec::new(),
            norun_components: Vec::new(),
            parser_component: None,
            rule_engine_component: None,
            enricher_component: None,
            output_component: None,
            dataset_manager: None,
            state_storage: None,
            alert_component : None,
            max_threads_parsing: max_threads,
            max_threads_enchancing: max_threads,
            max_threads_output: max_threads,
            max_threads_rule_engine: max_threads,
            command_response_timeout: command_timeout,
            command_map: BTreeMap::new(),
            dataset_map: BTreeMap::new(),
            task_map: BTreeMap::new(),
            metrics,
            dataset_channels: Arc::new(Mutex::new(BTreeMap::new())),
        };
    }

    fn register_component(&mut self, component: &Box<dyn SiemComponent>) {
        let caps = component.capabilities();
        let comp_name = component.name();
        
        for dataset in caps.datasets() {
            let dataset_type = dataset.name();
            if !self.dataset_map.contains_key(&dataset_type) {
                self.dataset_map
                    .insert(dataset_type.clone(), vec![comp_name.to_string()]);
            } else {
                match self.dataset_map.get_mut(dataset_type) {
                    Some(v) => {
                        v.push(comp_name.to_string());
                    }
                    None => {}
                }
            }
        }
        for comnd in caps.commands() {
            let data_name = comnd.name().to_string();
            if !self.command_map.contains_key(&data_name) {
                self.command_map.insert(data_name, vec![comp_name.to_string()]);
            } else {
                match self.command_map.get_mut(&data_name) {
                    Some(v) => {
                        v.push(comp_name.to_string());
                    }
                    None => {}
                }
            }
        }
        for task in caps.tasks() {
            let data_name = task.name().to_string();
            if !self.task_map.contains_key(&data_name) {
                self.task_map.insert(data_name, vec![comp_name.to_string()]);
            } else {
                match self.task_map.get_mut(&data_name) {
                    Some(v) => {
                        v.push(comp_name.to_string());
                    }
                    None => {}
                }
            }
        }
    }
    pub fn register_input_component(&mut self, component: Box<dyn SiemComponent>) {
        self.register_component(&component);
        self.input_components.push(component);
    }
    pub fn register_rule_engine_component(&mut self, component: Box<dyn SiemComponent>) {
        self.register_component(&component);
        self.rule_engine_component = Some(component);
    }
    pub fn register_output_component(&mut self, component: Box<dyn SiemComponent>) {
        self.register_component(&component);
        self.output_component = Some(component);
    }
    pub fn register_other_component(&mut self, component: Box<dyn SiemComponent>) {
        self.register_component(&component);
        self.other_components.push(component);
    }
    pub fn register_parser_component(&mut self, component: Box<dyn SiemComponent>) {
        self.register_component(&component);
        self.parser_component = Some(component);
    }
    pub fn register_enricher_component(&mut self, component: Box<dyn SiemComponent>) {
        self.register_component(&component);
        self.enricher_component = Some(component);
    }
    pub fn register_norun_component(&mut self, component: Box<dyn SiemComponent>) {
        self.norun_components.push(component);
    }
    pub fn register_dataset_manager(&mut self, component: Box<dyn SiemDatasetManager>) {
        self.dataset_manager = Some(component);
    }
    pub fn register_alert_component(&mut self, component: Box<dyn SiemComponent>) {
        self.alert_component = Some(component);
    }

    fn run_component(
        &self,
        mut component: Box<dyn SiemComponent>,
        datasets: DatasetHolder,
    ) -> (JoinHandle<()>, Sender<SiemMessage>) {
        let kernel_channel = self.own_channel.1.clone();
        component.set_kernel_sender(kernel_channel);
        let kernel_channel = self.own_channel.1.clone();
        match &self.state_storage {
            Some(state_storage) => {
                component.set_storage(state_storage.duplicate());
            }
            None => {} // Not gonna happen
        }
        let local_channel = component.local_channel();
        let comp_id = component.id();
        component.set_datasets(datasets);

        let mut required_datasets : Vec<&SiemDatasetType> = Vec::new();
        let capabilities = component.capabilities();
        for dts in capabilities.datasets() {
            required_datasets.push(dts.name());
        }
        let thread_join = thread::spawn(move || {
            component.run();
            let mut params = BTreeMap::new();
            params.insert(Cow::Borrowed("component_id"), Cow::Owned(comp_id.to_string()));
            let _e = kernel_channel.send(SiemMessage::Command(
                SiemCommandHeader{comp_id, comm_id : 0, user : String::from("kernel")},
                SiemCommandCall::OTHER(
                    Cow::Borrowed("COMPONENT_FINISHED"),
                    params
                ),
            ));
        });
        match self.dataset_channels.lock() {
            Ok(mut guard) => {
                for req_dataset_type in required_datasets {
                    if guard.contains_key(req_dataset_type) {
                        match guard.get_mut(req_dataset_type) {
                            Some(v) => {
                                v.push(local_channel.clone());
                            }
                            None => {}
                        }
                    } else {
                        guard.insert(req_dataset_type.clone(), vec![local_channel.clone()]);
                    }
                }
            }
            Err(_) => {}
        };
        (thread_join, local_channel)
    }

    /// If a component dies, then we need to recreate the Map of channels for the datasetmanager using the information of the components that are still running
    fn regenerate_dataset_channels(
        &mut self,
        thread_map: &BTreeMap<u64, (JoinHandle<()>, Sender<SiemMessage>)>,
        component_map: &BTreeMap<String, Vec<u64>>,
    ) {
        match self.dataset_channels.lock() {
            Ok(mut dataset_channels) => {
                for (dataset_name, comp_names) in self.dataset_map.iter() {
                    let mut channels = vec![];
                    for comp_name in comp_names {
                        match component_map.get(comp_name) {
                            Some(comp_ids) => {
                                for id in comp_ids {
                                    match thread_map.get(id) {
                                        Some((_, chan)) => {
                                            channels.push(chan.clone());
                                        }
                                        None => {}
                                    }
                                }
                            }
                            None => {}
                        }
                    }
                    dataset_channels.insert(dataset_name.clone(), channels);
                }
            }
            Err(_) => {
                //TODO
            }
        }
    }

    fn get_datasets(&self) -> DatasetHolder {
        let dataset_holder = match &self.dataset_manager {
            Some(dataset_manager) => dataset_manager.get_datasets(),
            None => {
                panic!("No DatasetManager!")
            }
        };
        dataset_holder
    }

    fn get_components_for_command(&self, call: &SiemCommandCall) -> Option<&Vec<String>> {
        let call_name =  match call {
            SiemCommandCall::START_COMPONENT(_) => "START_COMPONENT",
            SiemCommandCall::STOP_COMPONENT(_) => "STOP_COMPONENT",
            SiemCommandCall::FILTER_DOMAIN(_) => "FILTER_DOMAIN",
            SiemCommandCall::FILTER_EMAIL_SENDER(_) => {
                "FILTER_EMAIL_SENDER"
            }
            SiemCommandCall::FILTER_IP(_) => "FILTER_IP",
            SiemCommandCall::ISOLATE_ENDPOINT(_) => "ISOLATE_ENDPOINT",
            SiemCommandCall::ISOLATE_IP(_) => "ISOLATE_IP",
            SiemCommandCall::LOG_QUERY(_) => "LOG_QUERY",
            SiemCommandCall::OTHER(name, _) => &name,
            _ => ""
        };
        return self.command_map.get(call_name);
    }
    fn get_components_for_task(&self, task: &String) -> Option<&Vec<String>> {
        return self.task_map.get(task);
    }

    pub fn run(&mut self) {
        // First create the instances of each component
        let mut component_id: u64 = 0;
        // Component 0 = DatasetManager
        let mut thread_map: BTreeMap<u64, (JoinHandle<()>, Sender<SiemMessage>)> = BTreeMap::new();
        let mut component_map: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        let mut component_reverse_map: BTreeMap<u64, String> = BTreeMap::new();
        // ID = (Comp_ID, Timestamp)
        let mut command_response_track: BTreeMap<u64, (u64,u64, i64)> = BTreeMap::new();
        // ID = (Comp_ID, Timestamp)
        let mut task_response_track: BTreeMap<u64, (u64, i64)> = BTreeMap::new();
        let mut running_parsers = Vec::new();
        let mut running_enrichers = Vec::new();
        let mut running_outputs = Vec::new();
        let mut running_rule_engine = Vec::new();
        let mut command_id_gen = 0;
        let mut task_id_gen = 0;
        let mut datasets = self.get_datasets();
        let dataset_channels = Arc::clone(&self.dataset_channels);
        if self.input_components.len() == 0 {
            panic!("Kernel needs a InputComponent to receive logs!!!");
        }
        for input_comp in &self.input_components {
            let mut new_comp = (*input_comp).duplicate();
            component_id += 1;
            let comp_name = new_comp.name().to_string();
            new_comp.set_id(component_id);
            let (_s, r) = crossbeam_channel::bounded(1);
            let log_channel = self.parser_channel.1.clone();
            new_comp.set_log_channel(log_channel, r);
            let (thread_join, local_channel) = self.run_component(new_comp, datasets.clone());
            match component_map.get_mut(&comp_name) {
                Some(k) => {
                    k.push(component_id);
                }
                None => {
                    component_map.insert(comp_name.clone(), vec![component_id]);
                }
            }
            component_reverse_map.insert(component_id, comp_name);
            thread_map.insert(component_id, (thread_join, local_channel));
        }
        match &self.parser_component {
            Some(comp) => {
                let mut new_comp = (*comp).duplicate();
                component_id += 1;
                let comp_name = new_comp.name().to_string();
                new_comp.set_id(component_id);
                let r = self.parser_channel.0.clone();
                let s = self.enricher_channel.1.clone();
                new_comp.set_log_channel(s, r);
                let (thread_join, local_channel) = self.run_component(new_comp, datasets.clone());
                match component_map.get_mut(&comp_name) {
                    Some(k) => {
                        k.push(component_id);
                    }
                    None => {
                        component_map.insert(comp_name.clone(), vec![component_id]);
                    }
                }
                component_reverse_map.insert(component_id, comp_name);
                thread_map.insert(component_id, (thread_join, local_channel));
                running_parsers.push(component_id);
            }
            None => {
                panic!("Kernel needs a ParserComponent!!")
            }
        };
        match &self.enricher_component {
            Some(comp) => {
                let mut new_comp = (*comp).duplicate();
                component_id += 1;
                let comp_name = new_comp.name().to_string();
                new_comp.set_id(component_id);
                let r = self.enricher_channel.0.clone();
                let s = self.rule_engine_channel.1.clone();
                new_comp.set_log_channel(s, r);
                let (thread_join, local_channel) = self.run_component(new_comp, datasets.clone());
                match component_map.get_mut(&comp_name) {
                    Some(k) => {
                        k.push(component_id);
                    }
                    None => {
                        component_map.insert(comp_name.clone(), vec![component_id]);
                    }
                }
                component_reverse_map.insert(component_id, comp_name);
                thread_map.insert(component_id, (thread_join, local_channel));
                running_enrichers.push(component_id);
            }
            None => {
                panic!("Kernel needs a enricherComponent!!")
            }
        };
        match &self.rule_engine_component {
            Some(comp) => {
                let mut new_comp = (*comp).duplicate();
                component_id += 1;
                let comp_name = new_comp.name().to_string();
                new_comp.set_id(component_id);
                let r = self.rule_engine_channel.0.clone();
                let s = self.output_channel.1.clone();
                new_comp.set_log_channel(s, r);
                let (thread_join, local_channel) = self.run_component(new_comp, datasets.clone());
                match component_map.get_mut(&comp_name) {
                    Some(k) => {
                        k.push(component_id);
                    }
                    None => {
                        component_map.insert(comp_name.clone(), vec![component_id]);
                    }
                }
                component_reverse_map.insert(component_id, comp_name);
                thread_map.insert(component_id, (thread_join, local_channel));
                running_rule_engine.push(component_id);
            }
            None => {
                panic!("Kernel needs a RuleEngineComponent!!")
            }
        };
        match &self.output_component {
            Some(comp) => {
                let mut new_comp = (*comp).duplicate();
                component_id += 1;
                let comp_name = new_comp.name().to_string();
                new_comp.set_id(component_id);
                let r = self.output_channel.0.clone();
                let s = self.output_channel.1.clone();
                new_comp.set_log_channel(s, r);
                let (thread_join, local_channel) = self.run_component(new_comp, datasets.clone());
                match component_map.get_mut(&comp_name) {
                    Some(k) => {
                        k.push(component_id);
                    }
                    None => {
                        component_map.insert(comp_name.clone(), vec![component_id]);
                    }
                }
                component_reverse_map.insert(component_id, comp_name);
                thread_map.insert(component_id, (thread_join, local_channel));
                running_outputs.push(component_id);
            }
            None => {
                panic!("Kernel needs a OutputComponent!!")
            }
        };
        for comp in &self.other_components {
            let mut new_comp = (*comp).duplicate();
            component_id += 1;
            let comp_name = new_comp.name().to_string();
            new_comp.set_id(component_id);
            let (thread_join, local_channel) = self.run_component(new_comp, datasets.clone());
            match component_map.get_mut(&comp_name) {
                Some(k) => {
                    k.push(component_id);
                }
                None => {
                    component_map.insert(comp_name.clone(), vec![component_id]);
                }
            }
            component_reverse_map.insert(component_id, comp_name);
            thread_map.insert(component_id, (thread_join, local_channel));
        }
        // We can take the dataset manager. If for whatever reason the thread ends, then we will end the execution of the entire program.
        match self.dataset_manager.take() {
            Some(mut comp) => {
                let kernel_channel = self.own_channel.1.clone();
                comp.set_kernel_sender(kernel_channel);
                let local_channel = comp.local_channel();
                let thread_join = thread::spawn(move || {
                    comp.run();
                });
                thread_map.insert(0, (thread_join, local_channel));
            }
            None => {
                panic!("Kernel needs a ParserComponent!!")
            }
        };

        match &self.alert_component {
            Some(comp) => {
                let mut new_comp = (*comp).duplicate();
                component_id += 1;
                let comp_name = new_comp.name().to_string();
                new_comp.set_id(component_id);
                let (thread_join, local_channel) = self.run_component(new_comp, datasets.clone());
                match component_map.get_mut(&comp_name) {
                    Some(k) => {
                        k.push(component_id);
                    }
                    None => {
                        component_map.insert(comp_name.clone(), vec![component_id]);
                    }
                }
                component_reverse_map.insert(component_id, comp_name);
                thread_map.insert(component_id, (thread_join, local_channel));
            },
            None => {
                panic!("Kernel needs a AlertComponent!!")
            }
        };
        let mut total_messages = 0;
        loop {
            
            #[cfg(feature = "metrics")]
            {
                QUEUED_LOGS_PARSING.store(self.parser_channel.0.len() as i64, std::sync::atomic::Ordering::Relaxed);
                QUEUED_LOGS_ENRICHMENT.store(self.enricher_channel.0.len() as i64, std::sync::atomic::Ordering::Relaxed);
                QUEUED_LOGS_RULE_ENGINE.store(self.rule_engine_channel.0.len() as i64, std::sync::atomic::Ordering::Relaxed);
                QUEUED_LOGS_INDEXING.store(self.output_channel.0.len() as i64, std::sync::atomic::Ordering::Relaxed);
                QUEUED_MESSAGES_FOR_KERNEL.store(self.own_channel.0.len() as i64, std::sync::atomic::Ordering::Relaxed);
                TOTAL_MESSAGES_PROCESSED.store(total_messages, std::sync::atomic::Ordering::Relaxed);
            }
            // Get last dataset info
            let mut updated_datasets = false;
            // Check length of Receive Channels to increase the number of threads
            match self.parser_channel.0.capacity() {
                Some(capacity) => {
                    if (self.parser_channel.0.len() as f64) > (0.9 * (capacity as f64)) {
                        if running_parsers.len() < self.max_threads_parsing as usize {
                            match &self.parser_component {
                                Some(comp) => {
                                    if !updated_datasets {
                                        datasets = self.get_datasets();
                                        updated_datasets = true;
                                    }
                                    let mut new_comp = (*comp).duplicate();
                                    component_id += 1;
                                    running_parsers.push(component_id);
                                    let comp_name = new_comp.name().to_string();
                                    new_comp.set_id(component_id);
                                    let r = self.parser_channel.0.clone();
                                    let s = self.enricher_channel.1.clone();
                                    new_comp.set_log_channel(s, r);
                                    let (thread_join, local_channel) =
                                        self.run_component(new_comp, datasets.clone());
                                    match component_map.get_mut(&comp_name) {
                                        Some(k) => {
                                            k.push(component_id);
                                        }
                                        None => {
                                            component_map
                                                .insert(comp_name.clone(), vec![component_id]);
                                        }
                                    }
                                    component_reverse_map.insert(component_id, comp_name);
                                    thread_map.insert(component_id, (thread_join, local_channel));
                                }
                                None => {}
                            };
                        }
                    }
                }
                None => {} // Not posible
            }
            match self.enricher_channel.0.capacity() {
                Some(capacity) => {
                    if (self.enricher_channel.0.len() as f64) > (0.9 * (capacity as f64)) {
                        if running_enrichers.len() < self.max_threads_enchancing as usize {
                            match &self.enricher_component {
                                Some(comp) => {
                                    if !updated_datasets {
                                        datasets = self.get_datasets();
                                        updated_datasets = true;
                                    }
                                    let mut new_comp = (*comp).duplicate();
                                    component_id += 1;
                                    running_enrichers.push(component_id);
                                    let comp_name = new_comp.name().to_string();
                                    new_comp.set_id(component_id);
                                    let r = self.enricher_channel.0.clone();
                                    let s = self.rule_engine_channel.1.clone();
                                    new_comp.set_log_channel(s, r);
                                    let (thread_join, local_channel) =
                                        self.run_component(new_comp, datasets.clone());
                                    match component_map.get_mut(&comp_name) {
                                        Some(k) => {
                                            k.push(component_id);
                                        }
                                        None => {
                                            component_map
                                                .insert(comp_name.clone(), vec![component_id]);
                                        }
                                    }
                                    component_reverse_map.insert(component_id, comp_name);
                                    thread_map.insert(component_id, (thread_join, local_channel));
                                }
                                None => {}
                            };
                        }
                    }
                }
                None => {} // Not posible
            }
            match self.rule_engine_channel.0.capacity() {
                Some(capacity) => {
                    if (self.rule_engine_channel.0.len() as f64) > (0.9 * (capacity as f64)) {
                        if running_rule_engine.len() < self.max_threads_rule_engine as usize {
                            match &self.rule_engine_component {
                                Some(comp) => {
                                    if !updated_datasets {
                                        datasets = self.get_datasets();
                                        updated_datasets = true;
                                    }
                                    let mut new_comp = (*comp).duplicate();
                                    component_id += 1;
                                    running_rule_engine.push(component_id);
                                    let comp_name = new_comp.name().to_string();
                                    new_comp.set_id(component_id);
                                    let r = self.rule_engine_channel.0.clone();
                                    let s = self.output_channel.1.clone();
                                    new_comp.set_log_channel(s, r);
                                    let (thread_join, local_channel) =
                                        self.run_component(new_comp, datasets.clone());
                                    match component_map.get_mut(&comp_name) {
                                        Some(k) => {
                                            k.push(component_id);
                                        }
                                        None => {
                                            component_map
                                                .insert(comp_name.clone(), vec![component_id]);
                                        }
                                    }
                                    component_reverse_map.insert(component_id, comp_name);
                                    thread_map.insert(component_id, (thread_join, local_channel));
                                }
                                None => {}
                            };
                        }
                    }
                }
                None => {} // Not posible
            }
            match self.output_channel.0.capacity() {
                Some(capacity) => {
                    if (self.output_channel.0.len() as f64) > (0.9 * (capacity as f64)) {
                        if running_outputs.len() < self.max_threads_output as usize {
                            match &self.output_component {
                                Some(comp) => {
                                    if !updated_datasets {
                                        datasets = self.get_datasets();
                                    }
                                    let mut new_comp = (*comp).duplicate();
                                    component_id += 1;
                                    running_outputs.push(component_id);
                                    let comp_name = new_comp.name().to_string();
                                    new_comp.set_id(component_id);
                                    let r = self.output_channel.0.clone();
                                    let s = self.output_channel.1.clone();
                                    new_comp.set_log_channel(s, r);
                                    let (thread_join, local_channel) =
                                        self.run_component(new_comp, datasets.clone());
                                    match component_map.get_mut(&comp_name) {
                                        Some(k) => {
                                            k.push(component_id);
                                        }
                                        None => {
                                            component_map
                                                .insert(comp_name.clone(), vec![component_id]);
                                        }
                                    }
                                    component_reverse_map.insert(component_id, comp_name);
                                    thread_map.insert(component_id, (thread_join, local_channel));
                                }
                                None => {}
                            };
                        }
                    }
                }
                None => {} // Not posible
            }
            // Prioritize receiving messages
            for _ in 0..50 {
                match self.own_channel.0.try_recv() {
                    Ok(msg) => {
                        let mut for_kernel = false;
                        total_messages += 1;
                        match &msg {
                            SiemMessage::Command(comm_hdr, call) => {
                                let from_component = comm_hdr.comp_id;
                                match call {
                                    SiemCommandCall::START_COMPONENT(_comp_name) => {
                                        for_kernel = true;
                                        //TODO
                                    }
                                    SiemCommandCall::STOP_COMPONENT(comp_name) => {
                                        for_kernel = true;
                                        if comp_name == "KERNEL" {
                                            return
                                        }
                                        //TODO
                                    }
                                    SiemCommandCall::OTHER(name, _params) => {
                                        // Internal implementation of this kernel to detect when a component exits
                                        if name == "COMPONENT_FINISHED" {
                                            total_messages -= 1;// This does not count as a message
                                            for_kernel = true;
                                            // Remove all references to this component ID
                                            match component_reverse_map.get(&comm_hdr.comp_id) {
                                                Some(comp_name) => {
                                                    match component_map.get_mut(comp_name) {
                                                        Some(id_list) => {
                                                            id_list.retain(|value| {
                                                                *value != from_component
                                                            });
                                                        }
                                                        None => {}
                                                    }
                                                }
                                                None => {}
                                            }
                                            thread_map.remove(&comm_hdr.comp_id);
                                            self.regenerate_dataset_channels(
                                                &thread_map,
                                                &component_map,
                                            );
                                            //
                                            running_parsers.retain(|value| *value != from_component);
                                            running_enrichers
                                                .retain(|value| *value != from_component);
                                            running_rule_engine
                                                .retain(|value| *value != from_component);
                                            running_outputs.retain(|value| *value != from_component);
                                        }
                                    }
                                    _ => {}
                                }
                                if !for_kernel {
                                    match self.get_components_for_command(call) {
                                        Some(v) => {
                                            command_id_gen += 1;
                                            command_response_track.insert(
                                                command_id_gen,
                                                (comm_hdr.comp_id, comm_hdr.comm_id, chrono::Utc::now().timestamp_millis()),
                                            );
                                            for cmp in v {
                                                match component_map.get(cmp) {
                                                    Some(ids) => {
                                                        let selected_component = select_random_id(
                                                            command_id_gen as usize,
                                                            ids,
                                                        );
                                                        match thread_map.get(&selected_component) {
                                                            Some((_, channel)) => {
                                                                let _r = channel.send(
                                                                    SiemMessage::Command(
                                                                        SiemCommandHeader { comp_id : from_component, comm_id :command_id_gen, user : comm_hdr.user.clone()},
                                                                        call.clone(),
                                                                    ),
                                                                );
                                                            }
                                                            None => {}
                                                        }
                                                    }
                                                    None => {}
                                                }
                                            }
                                        }
                                        None => {}
                                    }
                                }
                            }
                            SiemMessage::Response(comm_hdr, response) => {
                                // Localize which component needs the response
                                match command_response_track.get(&comm_hdr.comm_id) {
                                    Some((resp_comp_id,resp_comm_id, timestamp)) => {
                                        let timestamp_now = chrono::Utc::now().timestamp_millis();
                                        // Update time metric ((timestamp_now - timestamp) as f64) / 1000.0
                                        if (timestamp_now - timestamp)
                                            > self.command_response_timeout
                                        {
                                            // Excessive response time. Do something
                                        } else {
                                            match thread_map.get(resp_comp_id) {
                                                Some((_, channel)) => {
                                                    let _r = channel.send(SiemMessage::Response(
                                                        SiemCommandHeader {comm_id : *resp_comm_id, comp_id : 0, user : comm_hdr.user.clone()},
                                                        response.clone(),
                                                    ));
                                                }
                                                None => {
                                                    // Component dead? => Do nothing with the response
                                                }
                                            }
                                        }
                                    },
                                    None => {}
                                }
                            }
                            SiemMessage::Alert(_alert) => {}
                            SiemMessage::Notification(_comp_id, _notification) => {}
                            SiemMessage::Task(comm_hdr, task) => {
                                match self.get_components_for_task(&task.origin) {
                                    Some(v) => {
                                        task_id_gen += 1;
                                        let timestamp = chrono::Utc::now().timestamp_millis();
                                        task_response_track
                                            .insert(task_id_gen, (comm_hdr.comp_id, timestamp));
                                        for cmp in v {
                                            match component_map.get(cmp) {
                                                Some(ids) => {
                                                    let selected_id =
                                                        select_random_id(task_id_gen as usize, ids);
                                                    let mut clonned_task = task.clone();
                                                    clonned_task.enqueued_at = timestamp;
                                                    clonned_task.id = task_id_gen;
                                                    match thread_map.get(&selected_id) {
                                                        Some((_, channel)) => {
                                                            let _r =
                                                                channel.send(SiemMessage::Task(
                                                                    SiemCommandHeader {comm_id : task_id_gen, comp_id : comm_hdr.comm_id, user : comm_hdr.user.clone()},
                                                                    clonned_task,
                                                                ));
                                                        }
                                                        None => {}
                                                    }
                                                }
                                                None => {}
                                            }
                                        }
                                    }
                                    None => {}
                                }
                            }
                            SiemMessage::TaskResult(task_hdr, task_res) => {
                                match task_response_track.get(&task_hdr.comm_id) {
                                    Some((comp_id, timestamp)) => {
                                        let timestamp_now = chrono::Utc::now().timestamp_millis();
                                        // TODO: update time metric ((timestamp_now - timestamp) as f64) / 1000.0
                                        if (timestamp_now - timestamp)
                                            > self.command_response_timeout
                                        {
                                            // Excessive response time. Do something
                                        } else {
                                            match thread_map.get(comp_id) {
                                                Some((_, channel)) => {
                                                    let _r = channel.send(SiemMessage::TaskResult(
                                                        SiemCommandHeader {comm_id : task_hdr.comm_id, comp_id : task_hdr.comp_id, user : String::from("kernel")},
                                                        task_res.clone(),
                                                    ));
                                                }
                                                None => {}
                                            }
                                        }
                                    }
                                    None => {}
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(err) => {
                        match err {
                            TryRecvError::Empty => {
                                // Continue
                            }
                            TryRecvError::Disconnected => {
                                panic!("Kernel channel disconnected!!!")
                            }
                        }
                        // Continue
                    }
                }
            }
        }
    }
    pub fn register_state_storage(&mut self, state_storage : Box<dyn SiemComponentStateStorage>) {
        self.state_storage = Some(state_storage);
    }

    pub fn get_metrics(&self) -> Vec<SiemMetricDefinition> {
        self.metrics.clone()
    }
}


fn select_random_id(id: usize, vc: &Vec<u64>) -> u64 {
    vc[id % vc.len()]
}

#[cfg(test)]
mod tests {

    use std::sync::atomic::Ordering;

    use super::*;
    use super::test_comp::{BasicComponent, BasicDatasetManager};

    fn setup_dummy_kernel() -> SiemBasicKernel {
        let mut kernel = SiemBasicKernel::new(1000, 4, 5000);
        let comp = BasicComponent::new();
        let dm = BasicDatasetManager::new();
        let ic = BasicComponent::new();
        let pc = BasicComponent::new();
        let ec = BasicComponent::new();
        let oc = BasicComponent::new();
        let re = BasicComponent::new();
        let ac = BasicComponent::new();
        kernel.register_other_component(Box::new(comp));
        kernel.register_dataset_manager(Box::new(dm));
        kernel.register_input_component(Box::new(ic));
        kernel.register_output_component(Box::new(oc));
        kernel.register_parser_component(Box::new(pc));
        kernel.register_rule_engine_component(Box::new(re));
        kernel.register_enricher_component(Box::new(ec));
        kernel.register_alert_component(Box::new(ac));
        kernel
    }

    #[test]
    fn test_kernel_instance() {
        let mut kernel = setup_dummy_kernel();
        let sender = kernel.own_channel.1.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            // STOP parser component to finish testing
            for _ in 0..20 {
                let _r = sender.send(SiemMessage::Command(
                    SiemCommandHeader{ comp_id : 0, comm_id : 0, user : String::from("kernel")},
                    SiemCommandCall::GET_RULE(String::from("no_exists_rule")),
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(300));
            let _r = sender.send(SiemMessage::Command(
                SiemCommandHeader{ comp_id : 0, comm_id : 0, user : String::from("kernel")},
                SiemCommandCall::STOP_COMPONENT("KERNEL".to_string()),
            ));
        });
        kernel.run();
        let mut metrics = BTreeMap::new();
        kernel.get_metrics().iter().for_each(|v| { metrics.insert(v.name.to_string(), v.metric.clone());});
        // Test metrics are working
        if let SiemMetric::Counter(val) = metrics.get("total_messages_processed").unwrap() {
            assert_eq!(20.0 as i64, val.load(Ordering::Relaxed));
        }else{ panic!("")}
        
        
        
    }
}
