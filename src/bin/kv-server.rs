#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;

use log::LevelFilter;
use simplekv::*;
use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::process::exit;
use structopt::StructOpt;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:6666";
const DEFAULT_ENGINE: Engine = Engine::kvstore;

arg_enum! {
#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Engine {
    kvstore,
    sled
}
}

#[derive(StructOpt, Debug)]
#[structopt(name = "kv-server")]
struct Opt {
    #[structopt(
        long,
        help = "Sets the listening address",
        value_name = "IP:PORT",
        raw(default_value = "DEFAULT_LISTENING_ADDRESS"),
        parse(try_from_str)
    )]
    addr: SocketAddr,
    #[structopt(
        long,
        help = "Sets the storage engine",
        value_name = "ENGINE-NAME",
        raw(possible_values = "&Engine::variants()")
    )]
    engine: Option<Engine>,
}

fn current_engine() -> Result<Option<Engine>> {
    let engine = current_dir()?.join("engine");
    if !engine.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!("The content of engine file is invalid: {}", e);
            Ok(None)
        }
    }
}

fn run_with_engine<E: KvEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let server = KvServer::new(engine);
    server.run(addr)
}

fn run(opt: Opt) -> Result<()> {
    let engine = opt.engine.unwrap_or(DEFAULT_ENGINE);
    info!("kv-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", opt.addr);

    // write engine to engine file
    fs::write(current_dir()?.join("engine"), format!("{}", engine))?;

    match engine {
        Engine::kvstore => run_with_engine(KvStore::open(current_dir()?)?, opt.addr),
        Engine::sled => {
            error!("not implement");
            Ok(())
        }
    }
}

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let mut opt = Opt::from_args();
    let res = current_engine().and_then(move |curr_engine| {
        if opt.engine.is_none() {
            opt.engine = curr_engine;
        }
        if curr_engine.is_some() && opt.engine != curr_engine {
            error!("Wrong engine!");
            exit(1);
        }
        run(opt)
    });
    if let Err(e) = res {
        error!("{}", e);
        exit(1);
    }
}
