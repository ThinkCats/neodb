use anyhow::{bail, Context, Error, Ok, Result};

use crate::context::{
    context_schema_info_update, context_scheme_data_update, context_set_insert_key,
    BINCODE_STR_FIXED_SIZE, CONTEXT, INSTALL_DIR, TABLE_ID,
};
use crate::parse::{ColDef, CreateTableDef, InsertDef, SelectDef};
use crate::store_file::{ColSchema, DataIdxEntry, TableSchema};
use convenient_skiplist::SkipList;
use sqlparser::ast::{DataType, Expr, Value};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::Path;

///install db process
pub fn install_meta_info_store() {
    let context = &CONTEXT.lock().unwrap().schema;
    //TODO cache file fd
    let mut file = check_or_create_file(context.path.as_str(), context.size).unwrap();
    let free_schema = context.meta_free;
    write_content(
        &mut file,
        free_schema.offset,
        &bincode::serialize(&free_schema.info).unwrap(),
    );

    //data offset
    let data_schema = context.meta_data;
    write_content(
        &mut file,
        data_schema.offset,
        &bincode::serialize(&data_schema.info).unwrap(),
    );
}

///load data info in schema file into mem
pub fn startup_load_schema_mem() {
    let context = &mut CONTEXT.lock().unwrap().schema;
    //TODO cache file fd
    let file = check_or_create_file(context.path.as_str(), context.size).unwrap();
    let init_offset = context.meta_data.info;
    println!("[debug] read data size:{}", init_offset);

    //read meta data
    let meta_data_cap = context.meta_data.capacity as usize;
    let meta_free_cap = context.meta_free.capacity as usize;
    let meta_total_cap = meta_data_cap + meta_free_cap;
    let mut meta_buf = vec![0; meta_total_cap];
    read_content(&file, 0, &mut meta_buf);
    let meta_data_buf = meta_buf.get(0..meta_data_cap).unwrap();
    let meta_free_buf = meta_buf.get(meta_data_cap..meta_total_cap).unwrap();
    let meta_data: u64 = bincode::deserialize(&meta_data_buf).unwrap();
    let meta_free: u64 = bincode::deserialize(&meta_free_buf).unwrap();
    println!(
        "[debug] read meta content:{:?}, data:{:?},free:{:?}",
        meta_buf, meta_data, meta_free
    );

    let schema_data_size = meta_data - meta_total_cap as u64;
    if schema_data_size == 0 {
        //schema data is blank
        return;
    }

    //update meme schema meta info
    context_schema_info_update(context, meta_free, meta_data);

    //read schema info
    let mut schema_buf = vec![0u8; schema_data_size as usize];
    let schema_start_offset = meta_total_cap as u64;
    read_content(&file, schema_start_offset, &mut schema_buf);
    println!("[debug] read content buf:{:?}", schema_buf);

    //let read_db: String = bincode::deserialize(&schema_buf).unwrap();
    let read_db = iter_buf(&mut schema_buf);
    println!("[debug] read content:{:?}", read_db);
    let mut skip_list = SkipList::new();
    for v in read_db {
        skip_list.insert(v);
    }
    //update mem schema data
    context_scheme_data_update(context, skip_list);
    println!(
        "[debug] after startup_load_schema_mem, context:{:?}",
        context
    );
}

pub fn iter_buf(buf: &mut [u8]) -> Vec<String> {
    //get fixed header
    let mut result: Vec<String> = Vec::new();
    let mut offset = 0;
    loop {
        if offset >= buf.len() {
            break;
        }

        let fixed_len = *BINCODE_STR_FIXED_SIZE as usize;
        let ele_lenth_buf = buf.get(offset..offset + fixed_len).unwrap();
        let ele_len: u64 = bincode::deserialize(ele_lenth_buf).unwrap();
        let ele_end_offset = fixed_len + offset + ele_len as usize;
        let ele_buf = buf.get(offset..ele_end_offset).unwrap();
        let content = bincode::deserialize(ele_buf).unwrap();

        result.push(content);
        offset = ele_end_offset;
    }
    return result;
}

///create a db
pub fn process_create_db(db_name: &str) {
    let context = &mut CONTEXT.lock().unwrap().schema;

    //check db exists
    if context.data.contains(&db_name.to_string()) {
        println!("schema has existed, abort.");
        return;
    }

    let meta_data = context.meta_data;
    let data_size = db_name.as_bytes().len() as u64;

    //TODO cache file fd
    let mut file = check_or_create_file(context.path.as_str(), context.size).unwrap();
    println!(
        "[debug] create db write file, position:{}, name:{}",
        meta_data.info, db_name
    );

    write_content(
        &mut file,
        meta_data.info,
        &bincode::serialize(&db_name).unwrap(),
    );

    //update data offset info and free info
    //because of bincode serialize, length add 8
    let data_info = meta_data.info + data_size + *BINCODE_STR_FIXED_SIZE as u64;

    println!(
        "[debug] create db write file data offset, position:{}, name:{}",
        meta_data.offset,
        format!("{}", data_info)
    );

    write_content(
        &mut file,
        meta_data.offset,
        &bincode::serialize(&data_info).unwrap(),
    );

    let free_info = context.meta_free.info - data_size;
    write_content(
        &mut file,
        context.meta_free.offset,
        &bincode::serialize(&free_info).unwrap(),
    );

    context_schema_info_update(context, free_info, data_info);

    //update mem skip_list, db not end with ;
    context.data.insert(db_name.to_string());

    //create schema table

    println!("[debug] after insert skip_list , context:{:?}", context);
}

///process create table
pub fn init_table_store(table_create_def: &CreateTableDef) {
    println!(
        "[debug] Start init table store process, get def:{:?}",
        table_create_def
    );
    //create table files, file name style: schema+table_name+col_name
    let table_name = &table_create_def.table_name;
    let cols = &table_create_def.columns;
    let schema = &CONTEXT.lock().unwrap().db_name;
    let mut col_schema_list: Vec<ColSchema> = Vec::new();

    for v in cols {
        //create data file
        let path = format!(
            "{}{}_{}_{}",
            *crate::context::INSTALL_DIR,
            schema,
            table_name,
            v.name
        );
        check_or_create_file(&path, 0).unwrap();
        //create index file
        if v.name != TABLE_ID.as_str() {
            let idx_path = format!(
                "{}{}_{}_{}_idx",
                *crate::context::INSTALL_DIR,
                schema,
                table_name,
                v.name
            );
            check_or_create_file(&idx_path, 0).unwrap();

            let tmp_col_schema = init_col_schema(&path, v);
            col_schema_list.push(tmp_col_schema);
        }
    }
    let table_schema_path = format!("{}{}_{}_schema", *INSTALL_DIR, schema, table_name);
    let mut table_schema_file = check_or_create_file(&table_schema_path, 0).unwrap();
    let table_schema = TableSchema { col_schema_list };
    let table_schema_buf = bincode::serialize(&table_schema).unwrap();
    write_content(&mut table_schema_file, 0, &table_schema_buf);
}

pub fn process_select(select_def: &SelectDef) -> Result<&str> {
    println!("store process select data:{:?}", select_def);
    let schema = &CONTEXT.lock().unwrap().db_name;
    let tables = &select_def.table;

    // TODO foreach table vec
    // for table in tables {
    let table = tables.get(0).unwrap();

    let mut cols: Vec<String> = Vec::new();

    //select * from xxxx; the cols is empty, should read from TableSchema
    if cols.is_empty() {
        let table_schema_path = format!("{}{}_{}_schema", *INSTALL_DIR, schema, table);
        let table_schema_file = check_or_create_file(&table_schema_path, 0).unwrap();
        let mut table_schema_buf = vec![0u8; table_schema_file.metadata().unwrap().len() as usize];
        read_content(&table_schema_file, 0, &mut table_schema_buf);
        let table_schema: TableSchema = bincode::deserialize(&table_schema_buf).unwrap();
        println!("select statement, table schema:{:?}", table_schema);
        let col_schema_list = table_schema.col_schema_list;
        let tmp_col: Vec<String> = col_schema_list.iter().map(|x| x.name.clone()).collect();
        cols = tmp_col.clone();
    } else {
        cols = select_def.columns.clone();
    }

    for col in cols {
        let data_path = format!(
            "{}{}_{}_{}",
            *crate::context::INSTALL_DIR,
            schema,
            table,
            col
        );
        let idx_path = format!(
            "{}{}_{}_{}_idx",
            *crate::context::INSTALL_DIR,
            schema,
            table,
            col
        );
        let idx_file_result = check_or_create_file(&idx_path, 0);
        if !idx_file_result.is_ok() {
            //col is not belong to the table
            continue;
        }
        let idx_file = idx_file_result.unwrap();
        let idx_file_len = idx_file.metadata().unwrap().len();
        if idx_file_len == 0 {
            continue;
        }
        let mut idx_buf = vec![0u8; idx_file_len as usize];
        read_content(&idx_file, 0, &mut idx_buf);
        let idx_data: Vec<DataIdxEntry> = bincode::deserialize(&idx_buf).unwrap();
        println!("select table idx data:{:?}", idx_data);
    }

    Ok("ok")
}

///insert table data
pub fn process_insert_data(insert_def: &InsertDef) -> Result<&str> {
    println!("store process insert data:{:?}", insert_def);

    let table = insert_def.table_name.to_string();
    let cols = insert_def.cols.to_vec();
    let data = insert_def.datas.to_vec();

    let mut context = CONTEXT.lock().unwrap();
    let schema = context.db_name.to_string();

    //data file
    for (idx, col) in cols.iter().enumerate() {
        let path = format!(
            "{}{}_{}_{}",
            *crate::context::INSTALL_DIR,
            schema,
            table,
            col
        );

        println!("[debug] parse file path:{}", path);
        //read col schema
        let mut file = check_or_create_file(&path, 0).unwrap();

        let mut buf = vec![0u8; ColSchema::CAP as usize];
        read_content(&file, ColSchema::DATA_OFFSET_CAP, &mut buf);

        let len: u64 = bincode::deserialize(&buf).unwrap();
        let mut schema_buf = vec![0u8; len as usize];
        read_content(&file, len, &mut schema_buf);
        let col_schema: ColSchema = bincode::deserialize(&schema_buf).unwrap();
        println!("[debug] read col schema len:{}, data:{:?}", len, col_schema);

        //get data
        for d in &data {
            let col_data = d.get(idx).unwrap();
            let value: Option<&String> = match col_data {
                Expr::Value(Value::SingleQuotedString(v)) => {
                    println!("v is:{}", v);
                    Some(v)
                }
                Expr::Value(Value::Number(v, _)) => {
                    println!("v num is:{}", v);
                    Some(v)
                }
                _ => None,
            };
            println!("current value:{:?}", value);

            if col_schema.name == "id" {
                let key = value.unwrap();
                let insert_info = &mut context.insert_info;
                context_set_insert_key(insert_info, (*key).to_string());

                continue;
            }

            let v_len = value.unwrap().len();
            if v_len > col_schema.len as usize {
                panic!("data too long");
            }

            //check type
            if col_schema.col_type.contains("CHARACTER") {
                //process char or varchar
            } else if col_schema.col_type.contains("INT") {
                //process int or bigint
            }

            let mut data_offset_buf = vec![0u8; ColSchema::DATA_OFFSET_CAP as usize];
            read_content(&file, 0, &mut data_offset_buf);
            let data_offset: u64 = bincode::deserialize(&data_offset_buf).unwrap();
            //store data -> free + value
            let encode_value = bincode::serialize(&value.unwrap()).unwrap();
            let free = col_schema.len - encode_value.len() as u64;
            if free <= 0 {
                panic!("data content too long")
            }
            let free_buf = bincode::serialize(&free).unwrap();
            write_content(&mut file, data_offset, &free_buf);
            write_content(
                &mut file,
                data_offset + free_buf.len() as u64,
                &encode_value,
            );

            let idx_path = format!(
                "{}{}_{}_{}_idx",
                *crate::context::INSTALL_DIR,
                schema,
                table,
                col_schema.name
            );
            let mut idx_file = check_or_create_file(&idx_path, 0).unwrap();
            let insert_key: u64 = *(&context
                .insert_info
                .insert_key
                .parse()
                .expect("key should a number"));

            let idx_entry = DataIdxEntry {
                key: insert_key,
                offset: data_offset,
            };

            let idx_file_size = idx_file.metadata().unwrap().len();
            let mut all_buf = vec![0u8; idx_file_size as usize];
            let mut all_data: Vec<DataIdxEntry> = Vec::new();
            if idx_file_size > 0 {
                read_content(&idx_file, 0, &mut all_buf);
                let mut all_data: Vec<DataIdxEntry> = bincode::deserialize(&all_buf).unwrap();
                println!("[debug] all index data:{:?}", all_data);
                all_data.push(idx_entry);
                let all_skip_list = SkipList::from_iter(all_data.iter());
                println!("[debug] all index skip list:{:?}", all_skip_list);
            } else {
                all_data = vec![idx_entry];
            }
            let all_data_buf = bincode::serialize(&all_data).unwrap();
            write_content(&mut idx_file, 0, &all_data_buf);

            println!("[debug] key is :{}", insert_key);
        }
    }
    Ok("ok")
}

fn init_col_schema(table_file_path: &str, col_def: &ColDef) -> ColSchema {
    let col_name = &col_def.name;
    println!("[debug] col name is :{}", col_name);
    let col_type = &col_def.col_type;
    let mut col_len: u64 = 0;
    match col_type {
        DataType::Varchar(opt) | DataType::BigInt(opt) => {
            println!("[debug] data type varchar len:{:?}", opt);
            match opt {
                Some(len) => {
                    col_len = *len;
                }
                None => {
                    col_len = 16;
                }
            }
        }
        _ => {
            println!("[debug] unsupport col type parse now");
        }
    }
    println!("[debug] data len:{}", col_len);
    let col_schema = crate::store_file::ColSchema {
        name: String::from(col_name),
        col_type: col_type.to_string(),
        len: col_len,
    };
    println!("[debug] col schema info:{:?}", col_schema);

    //write file
    let mut file = check_or_create_file(table_file_path, 0).unwrap();
    let bin = bincode::serialize(&col_schema).unwrap();
    let bin_len = bin.len() as u64;
    if bin_len > ColSchema::CAP {
        //TODO use result
        panic!("col definition is too long");
    }
    println!("[debug] bin len:{}", bin_len);

    //write data offset, TODO change col type and len will break here !!!
    let data_offset: u64 = ColSchema::CAP + ColSchema::DATA_OFFSET_CAP;
    let offset_buf = bincode::serialize(&data_offset).unwrap();
    write_content(&mut file, 0, &offset_buf);

    let schema_len = bincode::serialize(&bin_len).unwrap();
    //write schema len, after data offset area
    write_content(&mut file, ColSchema::DATA_OFFSET_CAP, &schema_len);

    //write schema
    write_content(&mut file, bin_len as u64, &bin);

    col_schema.clone()
}

fn file_exists(path: &str) -> bool {
    Path::new(path).exists()
}

pub fn check_or_create_file(path: &str, size: u64) -> Result<File> {
    if file_exists(path) {
        let f = OpenOptions::new()
            .write(true)
            .read(true)
            .open(path)
            .context("can not open file")
            .unwrap();
        return Ok(f);
    }
    let f = File::create(path)?;
    if size > 0 {
        f.set_len(size)?;
    }
    Ok(f)
}

pub fn append_content(f: &mut File, content: &[u8]) -> usize {
    let size = f.write(content).unwrap();
    f.flush().unwrap();
    return size;
}

pub fn write_content(f: &mut File, position: u64, content: &[u8]) -> usize {
    let size = f.write_at(content, position).unwrap();
    f.flush().unwrap();
    return size;
}

pub fn read_content(f: &File, position: u64, buf: &mut [u8]) {
    f.read_at(buf, position).unwrap();
}

pub fn delete_file(path: &str) {
    fs::remove_file(path).unwrap();
}
