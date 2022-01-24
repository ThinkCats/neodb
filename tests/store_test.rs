use ndb::store::check_or_create_file;

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
    let size = 1024 * 1024 * 1;
    let result = check_or_create_file(path, size);
    println!("result is:{:?}", result);
    assert_eq!(true, result.is_ok());
}
