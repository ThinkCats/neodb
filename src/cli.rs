use clap::Parser;
use std::io;
use std::io::stdout;
use std::io::{BufRead, Write};

use crate::context::{context_use_db, CONTEXT};
use crate::parse::{self, CreateDbDef, CreateTableDef, InsertDef, SelectDef};
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
    println!("[debug] args:{:?}", args);
    loop {
        print!("--> ");
        stdout().flush().unwrap();
        let mut buf = String::new();
        let stdin = io::stdin();
        let mut handle = stdin.lock();
        let result = handle.read_line(&mut buf);
        match result {
            Ok(_) => {
                //remove enter
                let buf_str = buf.as_str();
                let sub = buf_str[..buf_str.len() - 1].to_string();
                process_input_sql(sub);
            }
            Err(error) => {
                println!("err in cmd, info:{:?}", error);
            }
        }
    }
}

fn process_input_sql(input: String) {
    let result: Box<_>;
    let parse_result = parse::parse_sql(&input);
    match parse_result {
        Ok(data) => {
            result = data;
        }
        Err(err) => {
            println!("process sql error:{:?}", err);
            return;
        }
    }

    println!("[debug] Parse Your SQL Define:{}", result);
    let cmd = result.cmd();
    println!("[debug] Your SQL Cmd:{:?}", cmd);

    //check if use db
    if matches!(cmd, DbCmd::Use) {
        let use_def: &UseDef = match result.as_any().downcast_ref::<UseDef>() {
            Some(use_def) => use_def,
            None => panic!("parse sql result is not use def"),
        };
        context_use_db(use_def.db_name.as_str());
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
            let create_db_def: &CreateDbDef = match result.as_any().downcast_ref::<CreateDbDef>() {
                Some(create_db_def) => create_db_def,
                None => panic!("parse sql result is not create_db_def"),
            };
            println!("[debug] db name:{}", create_db_def.db_name);
            store::process_create_db(create_db_def.db_name.as_str());
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
        DbCmd::Insert => {
            println!("Start Table Insert ");
            let insert_def: &InsertDef = match result.as_any().downcast_ref::<InsertDef>() {
                Some(insert_def) => insert_def,
                None => panic!("parse sql result is not insert def"),
            };
            store::process_insert_data(insert_def).unwrap();
        }
        DbCmd::Select => {
            println!("Start Select Statement");
            let select_def: &SelectDef = match result.as_any().downcast_ref::<SelectDef>() {
                Some(select_def) => select_def,
                None => panic!("parse sql result is not select def"),
            };
            //TODO process select store
            store::process_select(select_def).unwrap();
        }
    }
}
