use convenient_skiplist::{RangeHint, SkipList};
use ndb::context::{ColSchema, CONTEXT};
use ndb::store::{
    check_or_create_file, delete_file, install_meta_info_store, iter_buf, process_create_db,
    read_content, startup_load_schema_mem, write_content,
};
use ndb::store_file::DataIdxEntry;

use std::io::{Read, Write};
use std::os::unix::prelude::FileExt;

#[test]
fn test_check_file() {
    let path = "/Users/wanglei/tmp/log/data.txt";
    let size = 10;
    let result = check_or_create_file(path, size);
    println!("result is:{:?}", result);
    assert_eq!(true, result.is_ok());
    delete_file(path);
}

#[test]
fn test_create_file() {
    let path = "/Users/wanglei/tmp/log/wal.log";
    let size = 10;
    let result = check_or_create_file(path, size);
    println!("result is:{:?}", result);
    assert_eq!(true, result.is_ok());
    delete_file(path);
}

#[test]
fn test_skip_list() {
    let mut list = SkipList::new();
    let entry1 = DataIdxEntry { key: 1, offset: 1 };
    println!("data entry size:{}", entry1.length());
    list.insert(entry1);

    let entry3 = DataIdxEntry { key: 3, offset: 3 };
    list.insert(entry3);

    let entry2 = DataIdxEntry { key: 2, offset: 2 };
    list.insert(entry2);

    println!("skip list detail :{:?}", list);
    assert_eq!(list.len(), 3);

    let range = list.range_with(|ele| {
        if ele.key > 2 {
            RangeHint::LargerThanRange
        } else if ele.key == 2 {
            RangeHint::InRange
        } else {
            RangeHint::SmallerThanRange
        }
    });

    for item in range {
        println!("range skip list:{:?}", item);
    }
}

//#[test]
//fn test_read_ff() {
//    let off = CONTEXT.lock().unwrap().schema.meta_data.offset;
//    let path = "/Users/wanglei/tmp/log/scheme";
//    let f = check_or_create_file(path, 1000).unwrap();
//    //let position = 0;
//    let mut buf = [0u8; 3];
//    let r = f.read_at(&mut buf, 0).unwrap();
//    println!(
//        "read :{:?}, r:{:?}, content:{:?}",
//        buf,
//        r,
//        String::from_utf8(buf.to_vec())
//    );
//}

#[test]
fn test_write_info() {
    let path = "/Users/wanglei/tmp/log/wal.log";
    let size = 1 * 1024;
    let mut f = check_or_create_file(path, size).unwrap();
    let content = bincode::serialize(
        "hellosasdfsdsdddddasdfhsd;lfkhas;dlkfhsd;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas;afhsdkjfhklsdjfhldskfalsdfgalsdkfglsdfadslfgdlskfgdslfafh;sdjhlajsdlkagflksdgflksdgfladddddddddddddda;shfskdjhflkas",
    )
    .unwrap();
    println!("content:{:?},len:{}", content, content.len());

    let mut times = 1;
    let mut position = 0;
    loop {
        if times > 1 {
            break;
        }
        let size = f.write_at(&content, position).unwrap();
        position += size as u64;
        f.flush().unwrap();
        times += 1;
    }
    //assert_eq!(position, 15);
    //delete_file(path);
}

#[test]
fn test_install_schema() {
    install_meta_info_store();
}

#[test]
fn test_process_create_db() {
    //create file
    install_meta_info_store();
    //read info
    startup_load_schema_mem();
    //create db
    process_create_db("hello");
    process_create_db("world");

    let context = &CONTEXT.lock().unwrap().schema;
    println!("scheme info :{:?}", context);

    assert_eq!(context.data.len(), 2);
    assert_eq!(context.meta_data.info, 12);

    println!("scheme info :{:?}", context);

    delete_file(context.path.as_str());
}

#[test]
fn test_iter_buf() {
    let mut d = vec![
        5, 0, 0, 0, 0, 0, 0, 0, 104, 119, 108, 108, 59, 3, 0, 0, 0, 0, 0, 0, 0, 103, 103, 59, 5, 0,
        0, 0, 0, 0, 0, 0, 116, 101, 115, 116, 59,
    ];
    let r = iter_buf(&mut d);
    println!("iter_buf result: {:?}", r);
}

#[test]
fn test_read_serial() {
    let path = "/Users/wanglei/tmp/log/hello_user_id";
    let mut file = check_or_create_file(path, 0).unwrap();

    let mut all_buf = vec![0u8; file.metadata().unwrap().len() as usize];
    file.read(&mut all_buf).unwrap();
    println!("all buf:{:?}", all_buf);

    let mut data_offset_buf = vec![0u8; 8];
    read_content(&file, 0, &mut data_offset_buf);
    let data_offset: u64 = bincode::deserialize(&data_offset_buf).unwrap();
    println!("read data offset buf:{:?}", data_offset_buf);
    println!("read data offset info:{:?}", data_offset);

    let mut d = vec![0u8; 256];
    read_content(&file, data_offset_buf.len() as u64, &mut d);
    println!("read header result:{:?}", d);
    let r: u64 = bincode::deserialize(&d).unwrap();
    println!("read header content:{}", r);

    let mut s = vec![0u8; r as usize];
    read_content(&file, r, &mut s);
    println!("read result:{:?}", s);
    let scheme: ColSchema = bincode::deserialize(&s).unwrap();
    println!("read scheme:{:?}", scheme);

    let entry = String::from("hehehe");
    let content = bincode::serialize(&entry).unwrap();
    println!("entry serialize len:{}", content.len());
    let d_pos = r + s.len() as u64;
    write_content(&mut file, d_pos, &content);

    let d2_pos = d_pos + content.len() as u64;
    let entry2 = String::from("world");
    let c_2 = bincode::serialize(&entry2).unwrap();
    write_content(&mut file, d2_pos, &c_2);

    let mut e = vec![0u8; content.len() as usize];
    read_content(&file, d_pos, &mut e);
    println!("read entry:{:?}", e);
    let r_e: String = bincode::deserialize(&e).unwrap();
    println!("read real entry:{:?}", r_e);

    let offset: u64 = 0;
    let offset_buf = bincode::serialize(&offset).unwrap();
    println!("offset buf size:{}", offset_buf.len());
}

#[test]
fn test_store_skiplist() {
    let d1 = DataIdxEntry { key: 1, offset: 1 };
    let d2 = DataIdxEntry { key: 2, offset: 2 };
    let d3 = DataIdxEntry { key: 3, offset: 3 };
    let mut list = Vec::new();
    list.push(d1);
    list.push(d2);
    list.push(d3);
    let file_path = String::from("/Users/wanglei/tmp/log/store_skiplist");
    let mut f = check_or_create_file(&file_path, 0).unwrap();
    let list_buf = bincode::serialize(&list).unwrap();
    write_content(&mut f, 0, &list_buf);

    let f_len = f.metadata().unwrap().len();
    let mut read_buf = vec![0u8; f_len as usize];
    read_content(&f, 0, &mut read_buf);
    println!("read buf:{:?}", read_buf);
    let read_info: Vec<DataIdxEntry> = bincode::deserialize(&read_buf).unwrap();
    println!("read info:{:?}", read_info);
    let skip_list = SkipList::from_iter(read_info.iter());
    println!("read skipList:{:?}", skip_list);
}
