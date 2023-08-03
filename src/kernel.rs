use channels::ComponentChannels;
use comp_store::SiemComponentStore;
use comp_tracking::{ComponentBuildingInfo, ComponentTracking};
use std::thread;
use usiem::components::command::SiemCommandCall;
use usiem::components::common::SiemMessage;
use usiem::components::metrics::SiemMetricDefinition;
use usiem::components::{SiemComponent, SiemDatasetManager};
use usiem::crossbeam_channel;
use usiem::crossbeam_channel::TryRecvError;
use usiem::prelude::kernel_message::KernelMessager;
use usiem::prelude::storage::SiemComponentStateStorage;
use usiem::prelude::{CommandResult, SiemCommandHeader, SiemCommandResponse};
use usiem::utilities::types::LogString;
use utils::ComponentNames;

use crate::metrics::{generate_kernel_metrics, KernelMetrics};
use crate::{channels, comp_store, comp_tracking, utils};

pub struct SiemBasicKernel {
    channels: ComponentChannels,
    components: SiemComponentStore,
    metrics: (Vec<SiemMetricDefinition>, KernelMetrics),
    pub command_response_timeout: i64,
    max_threads: usize,
    dataset_manager: Option<Box<dyn SiemDatasetManager>>,
}

impl SiemBasicKernel {
    pub fn new(channel_size: usize, max_threads: usize, command_timeout: i64) -> SiemBasicKernel {
        let metrics = generate_kernel_metrics();
        return SiemBasicKernel {
            channels: ComponentChannels::new(channel_size, max_threads as f64, metrics.1.clone()),
            components: SiemComponentStore::default(),
            command_response_timeout: command_timeout,
            max_threads,
            dataset_manager: None,
            metrics,
        };
    }

    pub fn register_wal_component(&mut self, component: Box<dyn SiemComponent>) {
        self.components.register_wal_component(component);
    }

    pub fn register_input_component(&mut self, component: Box<dyn SiemComponent>) {
        self.components.register_input_component(component);
    }
    pub fn register_rule_engine_component(&mut self, component: Box<dyn SiemComponent>) {
        self.components.register_rule_engine_component(component);
    }
    pub fn register_output_component(&mut self, component: Box<dyn SiemComponent>) {
        self.components.register_output_component(component);
    }
    pub fn register_other_component(&mut self, component: Box<dyn SiemComponent>) {
        self.components.register_other_component(component);
    }
    pub fn register_parser_component(&mut self, component: Box<dyn SiemComponent>) {
        self.components.register_parser_component(component);
    }
    pub fn register_enricher_component(&mut self, component: Box<dyn SiemComponent>) {
        self.components.register_enricher_component(component);
    }
    pub fn register_norun_component(&mut self, component: Box<dyn SiemComponent>) {
        self.components.register_norun_component(component);
    }
    pub fn register_dataset_manager(&mut self, component: Box<dyn SiemDatasetManager>) {
        self.dataset_manager = Some(component);
    }
    pub fn register_alert_component(&mut self, component: Box<dyn SiemComponent>) {
        self.components.register_alert_component(component);
    }
    pub fn register_state_storage(&mut self, state_storage: Box<dyn SiemComponentStateStorage>) {
        self.components.register_state_storage(state_storage);
    }

    fn increase_processed_messages(&self, value: u64) {
        self.metrics
            .1
            .total_messages_processed_by_kernel
            .inc_by(value as i64);
    }

    pub fn run(&mut self) {
        let mut component_tracking = ComponentTracking::new();
        component_tracking.command_timeout = self.command_response_timeout;

        let dataset_holder = match &self.dataset_manager {
            Some(dataset_manager) => dataset_manager.get_datasets(),
            None => {
                panic!("No DatasetManager!")
            }
        };

        // We can take the dataset manager. If for whatever reason the thread ends, then we will end the execution of the entire program.
        match self.dataset_manager.take() {
            Some(mut comp) => {
                let msngr = KernelMessager::new(
                    1,
                    comp.name().to_string(),
                    self.channels.kernel_channel.1.clone(),
                );
                let local_channel = comp.local_channel();
                let thread_join = thread::spawn(move || {
                    usiem::logging::initialize_component_logger(msngr);
                    comp.run()
                });
                component_tracking.register_external_component(1, (thread_join, local_channel));
            }
            None => {
                panic!("Kernel needs a ParserComponent!!")
            }
        };
        let guard = dataset_holder.lock().unwrap();
        let datasets = (*guard).clone();
        drop(guard);

        let mut build_info = ComponentBuildingInfo {
            store: self.components.clone(),
            channels: self.channels.clone(),
            datasets: datasets,
        };
        component_tracking.run_all_components(&build_info);

        let names = ComponentNames {
            parser: build_info.store.get_parser_name().to_string(),
            enricher: build_info.store.get_enricher_name().to_string(),
            rule_engine: build_info.store.get_rule_engine_name().to_string(),
            output: build_info.store.get_output_name().to_string(),
        };

        let mut iterations = 0;
        let mut total_messages_processed = 0;

        loop {
            iterations += 1;
            #[cfg(feature = "metrics")]
            {
                self.channels.update_metrics();
                self.increase_processed_messages(total_messages_processed);
                total_messages_processed = 0;
            }
            if iterations % 1024 == 0 {
                self.scale_components(&mut component_tracking, &names, &build_info);
            }

            if iterations % 64 == 0 {
                component_tracking.check_tasks();
            }
            // Prioritize receiving messages
            for _ in 0..50 {
                match self.channels.kernel_channel.0.try_recv() {
                    Ok(msg) => {
                        total_messages_processed += 1;
                        if self.is_message_for_kernel(&msg) {
                            match self.process_message_for_kernel(
                                msg,
                                &mut component_tracking,
                                &mut build_info,
                            ) {
                                Ok(_) => {}
                                Err(v) => {
                                    println!("{}", v);
                                    return;
                                }
                            }
                        } else {
                            let _ = component_tracking.route_message(msg, &build_info);
                        }
                    }
                    Err(err) => match err {
                        TryRecvError::Empty => {}
                        TryRecvError::Disconnected => {
                            panic!("Kernel channel disconnected!!!")
                        }
                    },
                }
            }
        }
    }

    fn process_message_for_kernel(
        &mut self,
        message: SiemMessage,
        component_tracking: &mut ComponentTracking,
        build_info: &mut ComponentBuildingInfo,
    ) -> Result<(), &'static str> {
        match message {
            SiemMessage::Command(header, command) => match command {
                SiemCommandCall::START_COMPONENT(_comp_name) => {}
                SiemCommandCall::STOP_COMPONENT(comp_name) => {
                    if comp_name == "KERNEL" {
                        return Err("Kernel received shutdown command");
                    }
                }
                SiemCommandCall::OTHER(name, _params) => {
                    if name == "COMPONENT_FINISHED" {
                        let _ = component_tracking.clean_comp_id(header.comp_id);
                    }
                }
                SiemCommandCall::GET_TASK_RESULT(task_id) => {
                    let result = component_tracking.get_task_result(task_id);
                    let result = if let Some(result) = result {
                        CommandResult::Ok(result)
                    } else {
                        CommandResult::Err(usiem::prelude::CommandError::NotFound(
                            LogString::Borrowed("Task has not finished"),
                        ))
                    };
                    component_tracking.send_message_to_component(
                        header.comp_id,
                        SiemMessage::Response(
                            SiemCommandHeader {
                                comm_id: header.comm_id,
                                comp_id: 0,
                                user: header.user,
                            },
                            SiemCommandResponse::GET_TASK_RESULT(result),
                        ),
                    );
                }
                _ => {}
            },
            SiemMessage::Notification(_) => {}
            SiemMessage::Dataset(dataset) => {
                component_tracking.update_dataset(dataset.clone());
                build_info.datasets.insert(dataset);
            }
            _ => {}
        }
        Ok(())
    }

    fn is_message_for_kernel(&self, message: &SiemMessage) -> bool {
        match message {
            SiemMessage::Command(_header, command) => match command {
                SiemCommandCall::START_COMPONENT(_comp_name) => true,
                SiemCommandCall::STOP_COMPONENT(_comp_name) => true,
                SiemCommandCall::OTHER(name, _params) => name == "COMPONENT_FINISHED",
                _ => false,
            },
            SiemMessage::Notification(_) => true,
            SiemMessage::Dataset(_dataset) => true,
            _ => false,
        }
    }

    fn scale_components(
        &self,
        tracking: &mut ComponentTracking,
        names: &ComponentNames,
        build_info: &ComponentBuildingInfo,
    ) {
        match self.channels.scale_parser() {
            channels::ScaleAction::ScaleUp => {
                if tracking.running_instances_of_component(&names.parser) < self.max_threads {
                    let _ = tracking.run_parser(&build_info);
                }
            }
            _ => {}
        };
        match self.channels.scale_enricher() {
            channels::ScaleAction::ScaleUp => {
                if tracking.running_instances_of_component(&names.enricher) < self.max_threads {
                    let _ = tracking.run_enricher(&build_info);
                }
            }
            _ => {}
        };
        match self.channels.scale_rules() {
            channels::ScaleAction::ScaleUp => {
                if tracking.running_instances_of_component(&names.rule_engine) < self.max_threads {
                    let _ = tracking.run_rule_engine(&build_info);
                }
            }
            _ => {}
        };
        match self.channels.scale_output() {
            channels::ScaleAction::ScaleUp => {
                if tracking.running_instances_of_component(&names.output) < self.max_threads {
                    let _ = tracking.run_output(&build_info);
                }
            }
            _ => {}
        };
    }

    pub fn get_metrics(&self) -> Vec<SiemMetricDefinition> {
        self.metrics.0.clone()
    }
    pub fn configure_channels_in_components(&mut self) {
        match self.components.wal_component.as_mut() {
            Some(comp) => {
                let r = self.channels.wal_log.0.clone();
                let s = self.channels.parser_channel.1.clone();
                comp.set_log_channel(s, r);
            }
            None => {}
        }
        match self.components.enricher_component.as_mut() {
            Some(comp) => {
                let r = self.channels.enricher_channel.0.clone();
                let s = self.channels.rule_engine_channel.1.clone();
                comp.set_log_channel(s, r);
            }
            None => {}
        }
        match self.components.parser_component.as_mut() {
            Some(comp) => {
                let r = self.channels.parser_channel.0.clone();
                let s = self.channels.enricher_channel.1.clone();
                comp.set_log_channel(s, r);
            }
            None => {}
        }
        match self.components.rule_engine_component.as_mut() {
            Some(comp) => {
                let r = self.channels.rule_engine_channel.0.clone();
                let s = self.channels.output_channel.1.clone();
                comp.set_log_channel(s, r);
            }
            None => {}
        }
        match self.components.output_component.as_mut() {
            Some(comp) => {
                let r = self.channels.rule_engine_channel.0.clone();
                let s = if let Some(_) = self.components.wal_component {
                    self.channels.wal_log.1.clone()
                } else {
                    let (s, _r) = crossbeam_channel::bounded(0);
                    s
                };
                comp.set_log_channel(s, r);
            }
            None => {}
        }
        for comp in self.components.input_components.iter_mut() {
            let (_s, r) = crossbeam_channel::bounded(1);
            let s = if let Some(_) = &self.components.wal_component {
                self.channels.wal_log.1.clone()
            } else {
                self.channels.parser_channel.1.clone()
            };
            comp.set_log_channel(s, r);
        }
    }
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use usiem::prelude::metrics::SiemMetric;
    use usiem::prelude::storage::DummyStateStorage;
    use usiem::prelude::SiemCommandHeader;

    use super::*;
    use crate::test_comp::{BasicComponent, BasicDatasetManager};

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
        kernel.register_state_storage(Box::new(DummyStateStorage {}));
        kernel
    }

    #[test]
    fn test_kernel_instance() {
        let mut kernel = setup_dummy_kernel();
        let sender = kernel.channels.kernel_channel.1.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(1_000));
            // STOP parser component to finish testing
            for _ in 0..20 {
                let _r = sender.send(SiemMessage::Command(
                    SiemCommandHeader {
                        comp_id: 0,
                        comm_id: 0,
                        user: String::from("kernel"),
                    },
                    SiemCommandCall::GET_RULE(String::from("no_exists_rule")),
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(1_000));
            let _r = sender.send(SiemMessage::Command(
                SiemCommandHeader {
                    comp_id: 0,
                    comm_id: 0,
                    user: String::from("kernel"),
                },
                SiemCommandCall::STOP_COMPONENT("KERNEL".to_string()),
            ));
        });
        kernel.run();
        let mut metrics = BTreeMap::new();
        kernel.get_metrics().iter().for_each(|v| {
            metrics.insert(v.name().to_string(), v.metric().clone());
        });
        // Test metrics are working
        if let SiemMetric::Counter(val) = metrics.get("total_messages_processed_by_kernel").unwrap()
        {
            // 20 messages of get rule + Notifications of components
            assert!(val.with_labels(&[]).unwrap().get() >= 20i64);
        } else {
            unreachable!("Must be a counter")
        }
    }
}
