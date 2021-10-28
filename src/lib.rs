use crossbeam_channel::{Receiver, Sender, TryRecvError};
use lazy_static::lazy_static;
use prometheus::{self, HistogramOpts, HistogramVec, IntGauge, Registry};
use serde_json::json;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use usiem::components::common::{
    SiemComponentStateStorage, SiemFunctionCall, SiemMessage,
};
use usiem::components::dataset::SiemDataset;
use usiem::components::{SiemComponent, SiemDatasetManager};
use usiem::events::SiemLog;

#[cfg(test)]
mod test_comp;

const COMMAND_RESPONSE_LABEL: &'static str = "command";
const TASK_RESPONSE_LABEL: &'static str = "task";

lazy_static! {
    // TODO: Use local variables for better performance
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref QUEUED_LOGS_PARSING: IntGauge =
        IntGauge::new("queued_logs_parsing", "Number of logs in the parsing queue")
            .expect("metric can be created");
    pub static ref QUEUED_LOGS_ENCHANCING: IntGauge = IntGauge::new(
        "queued_logs_enchancing",
        "Number of logs in the enchancing queue"
    )
    .expect("metric can be created");
    pub static ref QUEUED_LOGS_RULE_ENGINE: IntGauge = IntGauge::new(
        "queued_logs_rule_engine",
        "Number of logs in the rule engine queue"
    )
    .expect("metric can be created");
    pub static ref QUEUED_LOGS_INDEXING: IntGauge = IntGauge::new(
        "queued_logs_indexing",
        "Number of logs in the indexing queue"
    )
    .expect("metric can be created");
    pub static ref MESSAGES_FOR_KERNEL: IntGauge = IntGauge::new(
        "messages_for_kernel",
        "Number of messages that the kernel has in the queue"
    )
    .expect("metric can be created");
    pub static ref MESSAGE_RESPONSE_TIME: HistogramVec = HistogramVec::new(
        HistogramOpts::new("response_time", "Response Times"),
        &["message"]
    )
    .expect("metric can be created");
}

#[cfg(feature = "metrics")]
fn register_custom_metrics() {
    REGISTRY
        .register(Box::new(QUEUED_LOGS_PARSING.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(QUEUED_LOGS_ENCHANCING.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(QUEUED_LOGS_RULE_ENGINE.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(QUEUED_LOGS_INDEXING.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(MESSAGES_FOR_KERNEL.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(MESSAGE_RESPONSE_TIME.clone()))
        .expect("collector can be registered");
}

pub struct SiemBasicKernel {
    own_channel: (Receiver<SiemMessage>, Sender<SiemMessage>),
    parser_channel: (Receiver<SiemLog>, Sender<SiemLog>),
    enchancer_channel: (Receiver<SiemLog>, Sender<SiemLog>),
    rule_engine_channel: (Receiver<SiemLog>, Sender<SiemLog>),
    output_channel: (Receiver<SiemLog>, Sender<SiemLog>),
    input_components: Vec<Box<dyn SiemComponent>>,
    other_components: Vec<Box<dyn SiemComponent>>,
    norun_components: Vec<Box<dyn SiemComponent>>,
    parser_component: Option<Box<dyn SiemComponent>>,
    rule_engine_component: Option<Box<dyn SiemComponent>>,
    enchancer_component: Option<Box<dyn SiemComponent>>,
    output_component: Option<Box<dyn SiemComponent>>,
    dataset_manager: Option<Box<dyn SiemDatasetManager>>,
    state_storage: Option<Box<dyn SiemComponentStateStorage>>,
    pub max_threads_parsing: u64,
    pub max_threads_enchancing: u64,
    pub max_threads_output: u64,
    pub max_threads_rule_engine: u64,
    pub command_response_timeout: i64,
    command_map: BTreeMap<String, Vec<String>>,
    dataset_map: BTreeMap<String, Vec<String>>,
    task_map: BTreeMap<String, Vec<String>>,
    dataset_channels: Arc<Mutex<BTreeMap<String, Vec<Sender<SiemMessage>>>>>,
}

impl SiemBasicKernel {
    pub fn new(queue_size: usize, max_threads: u64, command_timeout: i64) -> SiemBasicKernel {
        let (os, or) = crossbeam_channel::bounded(queue_size);
        let (ps, pr) = crossbeam_channel::bounded(queue_size);
        let (es, er) = crossbeam_channel::bounded(queue_size);
        let (rs, rr) = crossbeam_channel::bounded(queue_size);
        let (is, ir) = crossbeam_channel::bounded(queue_size);
        return SiemBasicKernel {
            own_channel: (or, os),
            parser_channel: (pr, ps),
            enchancer_channel: (er, es),
            rule_engine_channel: (rr, rs),
            output_channel: (ir, is),
            input_components: Vec::new(),
            other_components: Vec::new(),
            norun_components: Vec::new(),
            parser_component: None,
            rule_engine_component: None,
            enchancer_component: None,
            output_component: None,
            dataset_manager: None,
            state_storage: None,
            max_threads_parsing: max_threads,
            max_threads_enchancing: max_threads,
            max_threads_output: max_threads,
            max_threads_rule_engine: max_threads,
            command_response_timeout: command_timeout,
            command_map: BTreeMap::new(),
            dataset_map: BTreeMap::new(),
            task_map: BTreeMap::new(),
            dataset_channels: Arc::new(Mutex::new(BTreeMap::new())),
        };
    }

    fn map_components(&mut self, component: &Box<dyn SiemComponent>) {
        let caps = component.capabilities();
        let comp_name = component.name().to_string();
        for dataset in caps.datasets() {
            let data_name = dataset.name().to_string();
            if !self.dataset_map.contains_key(&data_name) {
                self.dataset_map
                    .insert(data_name.to_string(), vec![comp_name.clone()]);
            } else {
                match self.dataset_map.get_mut(&data_name) {
                    Some(v) => {
                        v.push(comp_name.clone());
                    }
                    None => {}
                }
            }
        }
        for comnd in caps.commands() {
            let data_name = comnd.name().to_string();
            if !self.command_map.contains_key(&data_name) {
                self.command_map.insert(data_name, vec![comp_name.clone()]);
            } else {
                match self.command_map.get_mut(&data_name) {
                    Some(v) => {
                        v.push(comp_name.clone());
                    }
                    None => {}
                }
            }
        }
        for task in caps.tasks() {
            let data_name = task.name().to_string();
            if !self.task_map.contains_key(&data_name) {
                self.task_map.insert(data_name, vec![comp_name.clone()]);
            } else {
                match self.task_map.get_mut(&data_name) {
                    Some(v) => {
                        v.push(comp_name.clone());
                    }
                    None => {}
                }
            }
        }
    }
    pub fn register_input_component(&mut self, component: Box<dyn SiemComponent>) {
        self.map_components(&component);
        self.input_components.push(component);
    }
    pub fn register_rule_engine_component(&mut self, component: Box<dyn SiemComponent>) {
        self.map_components(&component);
        self.rule_engine_component = Some(component);
    }
    pub fn register_output_component(&mut self, component: Box<dyn SiemComponent>) {
        self.map_components(&component);
        self.output_component = Some(component);
    }
    pub fn register_other_component(&mut self, component: Box<dyn SiemComponent>) {
        self.map_components(&component);
        self.other_components.push(component);
    }
    pub fn register_parser_component(&mut self, component: Box<dyn SiemComponent>) {
        self.map_components(&component);
        self.parser_component = Some(component);
    }
    pub fn register_enchancer_component(&mut self, component: Box<dyn SiemComponent>) {
        self.map_components(&component);
        self.enchancer_component = Some(component);
    }
    pub fn register_norun_component(&mut self, component: Box<dyn SiemComponent>) {
        self.norun_components.push(component);
    }
    pub fn register_dataset_manager(&mut self, component: Box<dyn SiemDatasetManager>) {
        self.dataset_manager = Some(component);
    }

    fn run_component(
        &self,
        mut component: Box<dyn SiemComponent>,
        datasets: Vec<SiemDataset>,
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
        let component_id = component.id();
        component.set_datasets(datasets.clone());
        let mut datasets = Vec::new();
        for dts in component.capabilities().datasets() {
            datasets.push(dts.name().to_string());
        }
        let thread_join = thread::spawn(move || {
            component.run();
            let mut params = BTreeMap::new();
            params.insert(Cow::Borrowed("component_id"), Cow::Owned(component_id.to_string()));
            let _e = kernel_channel.send(SiemMessage::Command(
                component_id,
                0,
                SiemFunctionCall::OTHER(
                    Cow::Borrowed("COMPONENT_FINISHED"),
                    params
                ),
            ));
        });
        match self.dataset_channels.lock() {
            Ok(mut guard) => {
                for dataset in datasets {
                    if guard.contains_key(&dataset) {
                        match guard.get_mut(&dataset) {
                            Some(v) => {
                                v.push(local_channel.clone());
                            }
                            None => {}
                        }
                    } else {
                        guard.insert(dataset, vec![local_channel.clone()]);
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

    fn get_datasets(&self) -> Vec<SiemDataset> {
        let dataset_lock = match &self.dataset_manager {
            Some(dataset_manager) => dataset_manager.get_datasets(),
            None => {
                panic!("No DatasetManager!")
            }
        };
        let mut datasets = vec![];
        let dataset_guard = dataset_lock.lock().unwrap();
        for data in dataset_guard.iter() {
            datasets.push(data.clone());
        }
        drop(dataset_guard);
        drop(dataset_lock);
        return datasets;
    }

    fn get_components_for_command(&self, call: &SiemFunctionCall) -> Option<&Vec<String>> {
        let mut call_name = String::new();
        match call {
            SiemFunctionCall::START_COMPONENT(_) => call_name = String::from("START_COMPONENT"),
            SiemFunctionCall::STOP_COMPONENT(_) => call_name = String::from("STOP_COMPONENT"),
            SiemFunctionCall::FILTER_DOMAIN(_, _) => call_name = String::from("FILTER_DOMAIN"),
            SiemFunctionCall::FILTER_EMAIL_SENDER(_, _) => {
                call_name = String::from("FILTER_EMAIL_SENDER")
            }
            SiemFunctionCall::FILTER_IP(_, _) => call_name = String::from("FILTER_IP"),
            SiemFunctionCall::ISOLATE_ENDPOINT(_) => call_name = String::from("ISOLATE_ENDPOINT"),
            SiemFunctionCall::ISOLATE_IP(_) => call_name = String::from("ISOLATE_IP"),
            SiemFunctionCall::LOG_QUERY(_) => call_name = String::from("LOG_QUERY"),
            SiemFunctionCall::OTHER(name, _) => call_name = name.to_string(),
            _ => {}
        }
        return self.command_map.get(&call_name);
    }
    fn get_components_for_task(&self, task: &String) -> Option<&Vec<String>> {
        return self.task_map.get(task);
    }

    pub fn run(&mut self) {
        #[cfg(feature = "metrics")]
        {
            register_custom_metrics();
        }
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
        let mut running_enchancers = Vec::new();
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
                let s = self.enchancer_channel.1.clone();
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
        match &self.enchancer_component {
            Some(comp) => {
                let mut new_comp = (*comp).duplicate();
                component_id += 1;
                let comp_name = new_comp.name().to_string();
                new_comp.set_id(component_id);
                let r = self.enchancer_channel.0.clone();
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
                running_enchancers.push(component_id);
            }
            None => {
                panic!("Kernel needs a EnchancerComponent!!")
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

        match &self.dataset_manager {
            Some(comp) => {
                let mut new_comp = (*comp).duplicate();
                let kernel_channel = self.own_channel.1.clone();
                new_comp.set_kernel_sender(kernel_channel);
                new_comp.set_dataset_channels(dataset_channels);
                let local_channel = new_comp.local_channel();
                let thread_join = thread::spawn(move || {
                    new_comp.run();
                });
                thread_map.insert(0, (thread_join, local_channel));
            }
            None => {
                panic!("Kernel needs a ParserComponent!!")
            }
        };

        loop {
            #[cfg(feature = "metrics")]
            {
                QUEUED_LOGS_PARSING.set(self.parser_channel.0.len() as i64);
                QUEUED_LOGS_ENCHANCING.set(self.enchancer_channel.0.len() as i64);
                QUEUED_LOGS_RULE_ENGINE.set(self.rule_engine_channel.0.len() as i64);
                QUEUED_LOGS_INDEXING.set(self.output_channel.0.len() as i64);
                MESSAGES_FOR_KERNEL.set(self.own_channel.0.len() as i64);
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
                                    let s = self.enchancer_channel.1.clone();
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
            match self.enchancer_channel.0.capacity() {
                Some(capacity) => {
                    if (self.enchancer_channel.0.len() as f64) > (0.9 * (capacity as f64)) {
                        if running_enchancers.len() < self.max_threads_enchancing as usize {
                            match &self.enchancer_component {
                                Some(comp) => {
                                    if !updated_datasets {
                                        datasets = self.get_datasets();
                                        updated_datasets = true;
                                    }
                                    let mut new_comp = (*comp).duplicate();
                                    component_id += 1;
                                    running_enchancers.push(component_id);
                                    let comp_name = new_comp.name().to_string();
                                    new_comp.set_id(component_id);
                                    let r = self.enchancer_channel.0.clone();
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
                        match &msg {
                            SiemMessage::Command(comp_id,comm_id, call) => {
                                let for_component = *comp_id;
                                match call {
                                    SiemFunctionCall::START_COMPONENT(_comp_name) => {
                                        for_kernel = true;
                                        //TODO
                                    }
                                    SiemFunctionCall::STOP_COMPONENT(comp_name) => {
                                        for_kernel = true;
                                        if comp_name == "KERNEL" {
                                            return
                                        }
                                        //TODO
                                    }
                                    SiemFunctionCall::OTHER(name, _params) => {
                                        if name == "COMPONENT_FINISHED" {
                                            for_kernel = true;
                                            // Remove all references to this ID
                                            match component_reverse_map.get(comp_id) {
                                                Some(comp_name) => {
                                                    match component_map.get_mut(comp_name) {
                                                        Some(id_list) => {
                                                            id_list.retain(|value| {
                                                                *value != for_component
                                                            });
                                                        }
                                                        None => {}
                                                    }
                                                }
                                                None => {}
                                            }
                                            thread_map.remove(comp_id);
                                            self.regenerate_dataset_channels(
                                                &thread_map,
                                                &component_map,
                                            );
                                            //
                                            running_parsers.retain(|value| *value != for_component);
                                            running_enchancers
                                                .retain(|value| *value != for_component);
                                            running_rule_engine
                                                .retain(|value| *value != for_component);
                                            running_outputs.retain(|value| *value != for_component);
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
                                                (*comp_id, *comm_id, chrono::Utc::now().timestamp_millis()),
                                            );
                                            for cmp in v {
                                                match component_map.get(cmp) {
                                                    Some(ids) => {
                                                        let selected_id = select_random_id(
                                                            command_id_gen as usize,
                                                            ids,
                                                        );
                                                        match thread_map.get(&selected_id) {
                                                            Some((_, channel)) => {
                                                                let _r = channel.send(
                                                                    SiemMessage::Command(
                                                                        for_component,
                                                                        command_id_gen,
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
                            SiemMessage::Response(resp_id, response) => {
                                match command_response_track.get(resp_id) {
                                    Some((comp_id,comm_id, timestamp)) => {
                                        let timestamp_now = chrono::Utc::now().timestamp_millis();
                                        MESSAGE_RESPONSE_TIME
                                            .with_label_values(&[COMMAND_RESPONSE_LABEL])
                                            .observe(((timestamp_now - timestamp) as f64) / 1000.0);
                                        if (timestamp_now - timestamp)
                                            > self.command_response_timeout
                                        {
                                            // Excessive response time. Do something
                                        } else {
                                            match thread_map.get(comp_id) {
                                                Some((_, channel)) => {
                                                    let _r = channel.send(SiemMessage::Response(
                                                        *comm_id, // Replace the response ID with the original command ID
                                                        response.clone(),
                                                    ));
                                                }
                                                None => {}
                                            }
                                        }
                                    }
                                    None => {}
                                }
                            }
                            SiemMessage::Alert(_alert) => {}
                            SiemMessage::Notification(_comp_id, _notification) => {}
                            SiemMessage::Task(comp_id, task) => {
                                match self.get_components_for_task(&task.origin) {
                                    Some(v) => {
                                        task_id_gen += 1;
                                        let timestamp = chrono::Utc::now().timestamp_millis();
                                        task_response_track
                                            .insert(task_id_gen, (*comp_id, timestamp));
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
                                                                    task_id_gen,
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
                            SiemMessage::TaskResult(task_id, task_res) => {
                                match task_response_track.get(task_id) {
                                    Some((comp_id, timestamp)) => {
                                        let timestamp_now = chrono::Utc::now().timestamp_millis();
                                        MESSAGE_RESPONSE_TIME
                                            .with_label_values(&[TASK_RESPONSE_LABEL])
                                            .observe(((timestamp_now - timestamp) as f64) / 1000.0);
                                        if (timestamp_now - timestamp)
                                            > self.command_response_timeout
                                        {
                                            // Excessive response time. Do something
                                        } else {
                                            match thread_map.get(comp_id) {
                                                Some((_, channel)) => {
                                                    let _r = channel.send(SiemMessage::TaskResult(
                                                        *task_id,
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
}

fn select_random_id(id: usize, vc: &Vec<u64>) -> u64 {
    vc[id % vc.len()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::test_comp::{BasicComponent, BasicDatasetManager};

    #[test]
    fn test_kernel_instance() {
        let mut kernel = SiemBasicKernel::new(1000, 4, 5000);
        let comp = BasicComponent::new();
        let dm = BasicDatasetManager::new();
        let ic = BasicComponent::new();
        let pc = BasicComponent::new();
        let ec = BasicComponent::new();
        let oc = BasicComponent::new();
        let re = BasicComponent::new();
        kernel.register_other_component(Box::new(comp));
        kernel.register_dataset_manager(Box::new(dm));
        kernel.register_input_component(Box::new(ic));
        kernel.register_output_component(Box::new(oc));
        kernel.register_parser_component(Box::new(pc));
        kernel.register_rule_engine_component(Box::new(re));
        kernel.register_enchancer_component(Box::new(ec));
        let sender = kernel.own_channel.1.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            // STOP parser component to finish testing
            let _r = sender.send(SiemMessage::Command(
                0,
                0,
                SiemFunctionCall::STOP_COMPONENT(Cow::Borrowed("KERNEL")),
            ));
        });
        kernel.run();
    }
}
