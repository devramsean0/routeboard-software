use std::sync::LazyLock;

pub static OUTPUT_CONFIG: LazyLock<OutputConfig> = LazyLock::new(|| OutputConfig {
    strings: vec![OutputStrings {
        elements: vec![
            OutputElement { crs: "CHD" },
            OutputElement { crs: "SHF" },
            OutputElement { crs: "STP" },
        ],
    }],
});

pub struct OutputConfig {
    pub strings: Vec<OutputStrings>,
}

pub struct OutputStrings {
    pub elements: Vec<OutputElement>,
}

pub struct OutputElement {
    pub crs: &'static str,
}