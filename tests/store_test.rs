use ndb::store::{check_or_create_file, delete_file, init_meta_store, write_content};
use ndb::store_file::SSDataEntry;
use subway::skiplist::SkipList;

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
    list.insert(1, entry1);

    let entry3 = SSDataEntry {
        key: 3,
        value: String::from("Ssss"),
    };
    list.insert(3, entry3);

    let entry2 = SSDataEntry {
        key: 2,
        value: String::from("world"),
    };
    list.insert(2, entry2);
    let values = list.collect();
    println!("result:{:?}", values);
    assert_eq!(values.len(), 3);

    let dddd = list.get(&2);
    println!("second ele:{:?}", dddd);

    assert_eq!(dddd.unwrap().key, 2);
}

#[test]
fn test_write_info() {
    let path = "/Users/wanglei/tmp/log/wal.log";
    let size = 10;
    let f = check_or_create_file(path, size).unwrap();
    let content = "hello";
    let mut times = 1;
    let mut position = 0;
    loop {
        position += write_content(&f, position, content) as u64;
        times += 1;
        if times > 3 {
            break;
        }
    }
    assert_eq!(position, 15);
    delete_file(path);
}

#[test]
fn test_init_schema() {
    init_meta_store();
}
