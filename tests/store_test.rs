use convenient_skiplist::{RangeHint, SkipList};
use ndb::context::CONTEXT;
use ndb::store::{
    check_or_create_file, delete_file, install_meta_info_store, iter_buf, process_create_db,
    read_content, startup_load_schema_mem, write_content,
};
use ndb::store_file::SSDataEntry;

use std::io::Write;
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
    let entry1 = SSDataEntry {
        key: 1,
        value: String::from("hello"),
    };
    println!("data entry size:{}", entry1.length());
    list.insert(entry1);

    let entry3 = SSDataEntry {
        key: 3,
        value: String::from("Ssss"),
    };
    list.insert(entry3);

    let entry2 = SSDataEntry {
        key: 2,
        value: String::from("world"),
    };
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
