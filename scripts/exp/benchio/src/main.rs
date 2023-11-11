#![allow(warnings, unused)]

#[allow(unused_imports, dead_code)]
use std::time::{Duration, Instant};

use indicatif::*;
use kgdata::db::remotedb::shmemhelper::AllocatedMem;
use kgdata::db::remotedb::Client;
use kgdata::{
    db::{deser_entity, open_entity_db, Map, PredefinedDB, RemoteKGDB, RemoteRocksDBDict, KGDB},
    models::Entity,
};
use rayon::prelude::*;
const SOCKET_BASE_URL: &str = "ipc:///dev/shm/kgdata";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let inputdir = &args[1];
    let maxentsize = args[2].parse::<usize>().unwrap();
    let exptype = &args[3];

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

    let entfiles = (0..250)
        .into_iter()
        .map(|i| format!("{}/{}/entids.txt", inputdir, i))
        .collect::<Vec<_>>();

    let entlist = entfiles
        .iter()
        .map(|filename| {
            std::fs::read_to_string(filename)
                .unwrap()
                .lines()
                .filter(|x| x.len() > 0)
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .map(|x| x[0..maxentsize.min(x.len())].to_vec())
        .collect::<Vec<_>>();

    let mut start = Instant::now();

    if exptype == "thread" {
        fetch_ent_list_par(&entlist);
    }

    if exptype == "check" {
        check_system();
    }

    if exptype == "thread-2" {
        fetch_ent_list_par_2(&entlist);
    }

    if exptype == "thread-3" {
        fetch_ent_list_par_3(&entfiles, maxentsize);
    }

    if exptype == "process" {
        fetch_ent_list_proc_par(&entlist);
    }

    let duration = start.elapsed();
    println!("Main bench takes: {:?}", duration);
    println!("*** exptype,ncpus,nentdb,numthreads,batchsize,maxentsize,duration");
    println!(
        "**- {},{},{},{},{},{},{:?}",
        exptype,
        get_cpus(),
        get_n_entdb(),
        rayon::current_num_threads(),
        get_batch_size(),
        maxentsize,
        duration,
    )
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
        .map(|i| format!("{}/entity.{:0>3}.ipc", SOCKET_BASE_URL, i))
        .collect::<Vec<_>>();
    let entity_metadata_urls = (0..n_entmetadb)
        .into_iter()
        .map(|i| format!("{}/entity_metadata.{:0>3}.ipc", SOCKET_BASE_URL, i))
        .collect::<Vec<_>>();
    RemoteKGDB::new(&get_datadir(), &entity_urls, &entity_metadata_urls).unwrap()
}

fn get_entity_remote_db() -> RemoteRocksDBDict<String, Entity> {
    let n_entdb = get_n_entdb();
    let entity_urls = (0..n_entdb)
        .into_iter()
        .map(|i| format!("{}/entity.{:0>3}.ipc", SOCKET_BASE_URL, i))
        .collect::<Vec<_>>();
    RemoteRocksDBDict::new(&entity_urls, deser_entity).unwrap()
}

fn get_batch_size() -> usize {
    std::env::var("BATCH_SIZE")
        .unwrap_or("64".to_owned())
        .as_str()
        .parse::<usize>()
        .unwrap()
}

fn check_system() {
    let mut start = Instant::now();
    let entdb = get_entity_remote_db();
    for (i, socket) in entdb.sockets.iter().enumerate() {
        let blocks = socket.get_shm().unwrap().0.get_blocks();
        let free_blocks = blocks
            .iter()
            .filter(|block| AllocatedMem::is_free(block.mem, block.begin))
            .count();
        println!(
            "socket {} has {} blocks (#{} frees)",
            i,
            blocks.len(),
            free_blocks
        );
        for block in blocks {
            if !AllocatedMem::is_free(block.mem, block.begin) {
                println!("occupied block {}:{}", i, block.begin);
            }
        }
    }
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
                .par_slice_get(&entids, batch_size)
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

fn fetch_ent_list_par_2(entlist: &Vec<Vec<String>>) {
    let mut start = Instant::now();
    // let db = get_remote_db();
    // let entdb = &db.entities;
    // let entdb = get_entity_remote_db();
    let entdb = &get_db().entities;
    let batch_size = get_batch_size();
    println!("Load DB takes: {:?}", start.elapsed());

    start = Instant::now();
    let res = entlist
        .into_par_iter()
        .map(|entids| {
            entdb
                .par_slice_get(&entids, batch_size)
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

fn fetch_ent_list_par_3(entfiles: &Vec<String>, maxentsize: usize) {
    let mut start = Instant::now();
    // let db = get_remote_db();
    // let entdb = &db.entities;
    let entdb = get_entity_remote_db();
    // let entdb = &get_db().entities;
    let batch_size = get_batch_size();
    println!("Load DB takes: {:?}", start.elapsed());

    start = Instant::now();
    let res = entfiles
        .into_par_iter()
        .enumerate()
        .map(|(i, entfile)| {
            entdb
                .test(&format!("{}:{}", maxentsize, entfile), i)
                .unwrap()
        })
        .progress()
        .collect::<Vec<_>>();
    println!("Fetch entities in parallel takes: {:?}", start.elapsed());
    println!("Result: {}", res.into_iter().sum::<u32>())
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
                        db.slice_get(&arg)
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
