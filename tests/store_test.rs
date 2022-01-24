use ndb::store::check_or_create_file;
use ndb::store_file::SSDataEntry;
use subway::skiplist::SkipList;

#[test]
fn test_check_file() {
    let path = "/Users/wanglei/tmp/log/data.txt";
    let size = 1024 * 1024 * 1;
    let result = check_or_create_file(path, size);
    println!("result is:{:?}", result);
    assert_eq!(true, result.is_ok());
}

#[test]
fn test_create_file() {
    let path = "/Users/wanglei/tmp/log/wal.log";
    let size = 100;
    let result = check_or_create_file(path, size);
    println!("result is:{:?}", result);
    assert_eq!(true, result.is_ok());
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

    let dddd = list.get(&2);
    println!("second ele:{:?}", dddd);
}
