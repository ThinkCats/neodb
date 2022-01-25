use clap::Parser;
use std::io;
use std::io::stdout;
use std::io::{BufRead, Write};

use crate::context::{use_db, CONTEXT};
use crate::parse::{self, CreateTableDef};
use crate::parse::{DbCmd, UseDef};
use crate::store;

///ndb command
#[derive(Parser, Debug)]
#[clap(author,version,about, long_about= None)]
struct Args {
    ///server host
    #[clap(short, long, default_value = "localhost")]
    host: String,

    ///server port
    #[clap(short, long, default_value_t = 8888)]
    port: i16,
}

pub fn read_cmd() {
    let args = Args::parse();
    println!("args:{:?}", args);
    loop {
        print!("--> ");
        stdout().flush().unwrap();
        let mut buf = String::new();
        let stdin = io::stdin();
        let mut handle = stdin.lock();
        let result = handle.read_line(&mut buf);
        match result {
            Ok(input) => {
                println!("Input:{},Size:{}", &buf, input);
                process_input_sql(buf);
            }
            Err(error) => {
                println!("err in cmd, info:{:?}", error);
            }
        }
    }
}

fn process_input_sql(input: String) {
    let result = parse::parse_sql(&input);
    println!("Parse Your SQL:{}", result);
    let cmd = result.cmd();
    println!("Your SQL Cmd:{:?}", cmd);

    //check if use db
    if matches!(cmd, DbCmd::Use) {
        let use_def: &UseDef = match result.as_any().downcast_ref::<UseDef>() {
            Some(use_def) => use_def,
            None => panic!("parse sql result is not use def"),
        };
        use_db(use_def.db_name.as_str());
    }

    if !matches!(cmd, DbCmd::CreateDatabase) {
        if "" == CONTEXT.lock().unwrap().db_name {
            println!("please select your database");
            return;
        }
    }

    match cmd {
        DbCmd::Use => {}
        DbCmd::Show => {
            println!("Process Show Statement");
        }
        DbCmd::CreateDatabase => {
            println!("Start Init Create Db Schema");
            store::init_meta_store();
        }
        DbCmd::CreateTable => {
            println!("Start Create Table Init");
            let create_table_def: &CreateTableDef =
                match result.as_any().downcast_ref::<CreateTableDef>() {
                    Some(create_table_def) => create_table_def,
                    None => panic!("parse sql result is not create_table_def"),
                };
            store::init_table_store(create_table_def);
        }
        DbCmd::Select => {}
    }
}

fn process_use_db() {}
