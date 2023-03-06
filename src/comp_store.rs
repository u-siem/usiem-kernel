use std::collections::BTreeMap;

use usiem::prelude::{
    storage::SiemComponentStateStorage, task::{SiemTaskType, TaskDefinition}, SiemCommandCall, SiemComponent,
    SiemDatasetType, SiemFunctionType,
};

#[derive(Default)]
pub struct SiemComponentStore {
    pub input_components: Vec<Box<dyn SiemComponent>>,
    pub wal_component: Option<Box<dyn SiemComponent>>,
    /// Other components not used in the log processing pipeline
    pub other_components: Vec<Box<dyn SiemComponent>>,
    /// Components that does not need to be run
    pub norun_components: Vec<Box<dyn SiemComponent>>,
    /// Component that parses logs
    pub parser_component: Option<Box<dyn SiemComponent>>,
    /// Component that applies rules to the logs
    pub rule_engine_component: Option<Box<dyn SiemComponent>>,
    /// Component that enriches logs with information extracted from datasets or updates the datasets
    pub enricher_component: Option<Box<dyn SiemComponent>>,
    /// Component that stores logs or querys them
    pub output_component: Option<Box<dyn SiemComponent>>,
    /// Component that allows to store or retrieve information
    pub state_storage: Option<Box<dyn SiemComponentStateStorage>>,
    /// Alerting component
    pub alert_component: Option<Box<dyn SiemComponent>>,

    command_map: BTreeMap<SiemFunctionType, Vec<String>>,
    dataset_map: BTreeMap<SiemDatasetType, Vec<String>>,
    task_map: BTreeMap<SiemTaskType, Vec<TaskDefinition>>,
}

impl Clone for SiemComponentStore {
    fn clone(&self) -> Self {
        let mut task_map = BTreeMap::new();
        self.task_map.iter().for_each(|(k,v)| {
            task_map.insert(k.clone(), v.iter().map(|b| (*b).clone()).collect());
        });

        Self {
            input_components: self.input_components.iter().map(|v| v.duplicate()).collect(),
            wal_component: self.wal_component.as_ref().and_then(|v| Some(v.duplicate())),
            other_components: self.other_components.iter().map(|v| v.duplicate()).collect(),
            norun_components: self.norun_components.iter().map(|v| v.duplicate()).collect(),
            parser_component: self.parser_component.as_ref().and_then(|v| Some(v.duplicate())),
            rule_engine_component: self.rule_engine_component.as_ref().and_then(|v| Some(v.duplicate())),
            enricher_component: self.enricher_component.as_ref().and_then(|v| Some(v.duplicate())),
            output_component: self.output_component.as_ref().and_then(|v| Some(v.duplicate())),
            state_storage: self.state_storage.as_ref().and_then(|v| Some(v.duplicate())),
            alert_component: self.alert_component.as_ref().and_then(|v| Some(v.duplicate())),
            command_map: self.command_map.clone(),
            dataset_map: self.dataset_map.clone(),
            task_map,
        }
    }
}

impl SiemComponentStore {
    pub fn register_wal_component(&mut self, component: Box<dyn SiemComponent>) {
        self.register_component(&component);
        self.wal_component = Some(component);
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
        self.register_component(&component);
        self.norun_components.push(component);
    }
    pub fn register_alert_component(&mut self, component: Box<dyn SiemComponent>) {
        self.register_component(&component);
        self.alert_component = Some(component);
    }

    pub fn register_state_storage(&mut self, state_storage: Box<dyn SiemComponentStateStorage>) {
        self.state_storage = Some(state_storage);
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
            let data_name = comnd.class();
            if !self.command_map.contains_key(data_name) {
                self.command_map
                    .insert(data_name.clone(), vec![comp_name.to_string()]);
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
            let data_name = task.data().class();
            if !self.task_map.contains_key(&data_name) {
                self.task_map.insert(data_name, vec![task.clone()]);
                
            } else {
                match self.task_map.get_mut(&data_name) {
                    Some(v) => {
                        v.push(task.clone());
                    }
                    None => {}
                }
            }
        }
    }

    pub fn get_components_for_command(&self, call: &SiemCommandCall) -> Option<&Vec<String>> {
        return self.command_map.get(&call.get_type());
    }
    pub fn get_definitions_for_task(&self, task: &SiemTaskType) -> Option<&Vec<TaskDefinition>> {
        return self.task_map.get(task);
    }

    pub fn get_components_for_dataset(&self, dataset: &SiemDatasetType) -> Option<&Vec<String>> {
        return self.dataset_map.get(dataset);
    }

    pub fn _get_component_by_name(&self, name: &str) -> Option<&Box<dyn SiemComponent>> {
        if let Some(comp) = &self.alert_component {
            if comp.name() == name {
                return Some(comp)
            }
        }
        if let Some(comp) = &self.enricher_component {
            if comp.name() == name {
                return Some(comp)
            }
        }
        if let Some(comp) = &self.parser_component {
            if comp.name() == name {
                return Some(comp)
            }
        }
        if let Some(comp) = &self.output_component {
            if comp.name() == name {
                return Some(comp)
            }
        }
        if let Some(comp) = &self.wal_component {
            if comp.name() == name {
                return Some(comp)
            }
        }
        if let Some(comp) = &self.rule_engine_component {
            if comp.name() == name {
                return Some(comp)
            }
        }
        for comp in &self.input_components {
            if comp.name() == name {
                return Some(comp)
            }
        }
        for comp in &self.other_components {
            if comp.name() == name {
                return Some(comp)
            }
        }
        for comp in &self.norun_components {
            if comp.name() == name {
                return Some(comp)
            }
        }
        None
    }

    pub fn get_parser_name(&self) -> &str {
        self.parser_component
            .as_ref()
            .and_then(|v| Some(v.name()))
            .unwrap_or("")
    }
    pub fn get_enricher_name(&self) -> &str {
        self.enricher_component
            .as_ref()
            .and_then(|v| Some(v.name()))
            .unwrap_or("")
    }
    pub fn get_output_name(&self) -> &str {
        self.output_component
            .as_ref()
            .and_then(|v| Some(v.name()))
            .unwrap_or("")
    }
    pub fn get_rule_engine_name(&self) -> &str {
        self.rule_engine_component
            .as_ref()
            .and_then(|v| Some(v.name()))
            .unwrap_or("")
    }
}
