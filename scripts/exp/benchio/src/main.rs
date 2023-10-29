#[allow(unused_imports)]
use std::time::{Duration, Instant};

use indicatif::*;
use kgdata::{
    db::{deser_entity, open_entity_db, Dict, PredefinedDB, RemoteKGDB, RemoteRocksDBDict, KGDB},
    models::Entity,
};
use rayon::prelude::*;

const SOCKET_BASE_URL: &str = "ipc:///dev/shm/kgdata";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let inputdir = &args[1];
    let exptype = &args[2];

    if exptype == "process" {
        procspawn::init();
    }

    println!(
        "args: {:?} -- N_CPUS={} -- N_ENTDB={} -- NUM_THREADS={} -- BATCH_SIZE={}",
        args,
        get_cpus(),
        get_n_entdb(),
        rayon::current_num_threads(),
        get_batch_size()
    );

    let entlist = (0..250)
        .into_iter()
        .map(|i| {
            let filename = format!("{}/{}/entids.txt", inputdir, i);
            std::fs::read_to_string(&filename)
                .unwrap()
                .lines()
                .filter(|x| x.len() > 0)
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .map(|x| x[0..5000.min(x.len())].to_vec())
        .collect::<Vec<_>>();

    let mut start = Instant::now();

    if exptype == "thread" {
        fetch_ent_list_par(&entlist);
    }

    if exptype == "process" {
        fetch_ent_list_proc_par(&entlist);
    }

    println!("Main bench takes: {:?}", start.elapsed());
}

fn get_datadir() -> String {
    format!("{}/kgdata/databases/wikidata/20230619", "/var/tmp")
}

fn get_cpus() -> usize {
    std::env::var("N_CPUS")
        .unwrap_or(num_cpus::get().to_string())
        .as_str()
        .parse::<usize>()
        .unwrap()
}

fn get_n_entdb() -> usize {
    std::env::var("N_ENTDB")
        .unwrap_or(num_cpus::get().to_string())
        .as_str()
        .parse::<usize>()
        .unwrap()
}

fn get_db() -> KGDB {
    KGDB::new(&get_datadir()).unwrap()
}

fn get_remote_db() -> RemoteKGDB {
    let n_entdb = get_n_entdb();
    let n_entmetadb = std::env::var("N_ENTMETADB")
        .unwrap_or(num_cpus::get().to_string())
        .as_str()
        .parse::<usize>()
        .unwrap();

    let entity_urls = (0..n_entdb)
        .into_iter()
        .map(|i| format!("{}/ent.{:0>3}.ipc", SOCKET_BASE_URL, i))
        .collect::<Vec<_>>();
    let entity_metadata_urls = (0..n_entmetadb)
        .into_iter()
        .map(|i| format!("{}/entmeta.{:0>3}.ipc", SOCKET_BASE_URL, i))
        .collect::<Vec<_>>();
    RemoteKGDB::new(&get_datadir(), &entity_urls, &entity_metadata_urls).unwrap()
}

fn get_entity_remote_db() -> RemoteRocksDBDict<String, Entity> {
    let n_entdb = get_n_entdb();
    let entity_urls = (0..n_entdb)
        .into_iter()
        .map(|i| format!("{}/ent.{:0>3}.ipc", SOCKET_BASE_URL, i))
        .collect::<Vec<_>>();
    RemoteRocksDBDict::new(&entity_urls, deser_entity).unwrap()
}

fn get_batch_size() -> usize {
    std::env::var("BATCH_SIZE")
        .unwrap_or(num_cpus::get().to_string())
        .as_str()
        .parse::<usize>()
        .unwrap()
}

fn fetch_ent_list_par(entlist: &Vec<Vec<String>>) {
    let mut start = Instant::now();
    // let db = get_remote_db();
    // let entdb = &db.entities;
    let entdb = get_entity_remote_db();
    let batch_size = get_batch_size();
    println!("Load DB takes: {:?}", start.elapsed());

    start = Instant::now();
    let res = entlist
        .into_par_iter()
        .map(|entids| {
            entdb
                .par_batch_get(&entids, batch_size)
                .unwrap()
                .into_iter()
                .map(|ent| ent.unwrap().id[1..].parse::<i32>().unwrap() % 2)
                .collect::<Vec<_>>()
        })
        .progress()
        .collect::<Vec<_>>();
    println!("Fetch entities in parallel takes: {:?}", start.elapsed());
    println!("Result: {}", res.into_iter().flatten().sum::<i32>())
}

fn fetch_ent_list_proc_par(entlist: &Vec<Vec<String>>) {
    let start = Instant::now();
    let chunks = get_cpus();

    let jobs = (0..chunks)
        .into_par_iter()
        .map(|idx| {
            let args = entlist
                .iter()
                .enumerate()
                .filter(|(i, _)| i % chunks == idx)
                .map(|(_, x)| x.clone())
                .collect::<Vec<_>>();

            procspawn::spawn(args, |args: Vec<Vec<String>>| {
                let db = &get_db().entities;
                args.into_iter()
                    .map(|arg| {
                        db.batch_get(&arg)
                            .unwrap()
                            .into_iter()
                            .map(|ent| ent.unwrap().id[1..].parse::<i32>().unwrap() % 2)
                            .collect::<Vec<_>>()
                    })
                    .flatten()
                    .collect::<Vec<_>>()
            })
        })
        .progress()
        .collect::<Vec<_>>();

    let res = jobs
        .into_iter()
        .map(|job| job.join().unwrap())
        .progress()
        .flatten()
        .collect::<Vec<_>>();
    println!(
        "Fetch entities in proc parallel takes: {:?}",
        start.elapsed()
    );
    println!("Result: {}", res.into_iter().sum::<i32>())
}
