use convenient_skiplist::{RangeHint, SkipList};
use ndb::context::CONTEXT;
use ndb::store::{
    check_or_create_file, delete_file, install_meta_info_store, process_create_db,
    startup_load_schema_mem, write_content,
};
use ndb::store_file::SSDataEntry;

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

#[test]
fn test_write_info() {
    let path = "/Users/wanglei/tmp/log/wal.log";
    let size = 10;
    let mut f = check_or_create_file(path, size).unwrap();
    let content = "hello";
    let mut times = 1;
    let mut position = 0;
    loop {
        position += write_content(&mut f, position, content) as u64;
        times += 1;
        if times > 3 {
            break;
        }
    }
    assert_eq!(position, 15);
    delete_file(path);
}

//#[test]
//fn test_install_schema() {
//    install_meta_info_store();
//}

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
    assert_eq!(context.schema_data.info, 12);

    println!("scheme info :{:?}", context);

    delete_file(context.path.as_str());
}
