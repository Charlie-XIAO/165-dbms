use bitflags::bitflags;

bitflags! {
    pub struct ScanCallbackFlags: u8 {
        const SELECT = 0x01;
        const MIN = 0x02;
        const MAX = 0x04;
        const SUM = 0x08;
    }
}

pub const SOCK_PATH: &str = match option_env!("SOCK_PATH") {
    Some(val) => val,
    None => "cs165_unix_socket",
};

pub const DB_PERSIST_DIR: &str = match option_env!("DB_PERSIST_DIR") {
    Some(val) => val,
    None => ".cs165_db",
};

pub const DB_PERSIST_CATALOG_FILE: &str = match option_env!("DB_PERSIST_CATALOG_FILE") {
    Some(val) => val,
    None => "__catalog__",
};

pub const NUM_ELEMS_PER_LOAD_BATCH: usize = 4096;

pub const INIT_NUM_ELEMS_IN_JOIN_RESULT: usize = 1024;
