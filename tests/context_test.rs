use ndb::context::{context_use_db, CONTEXT};

#[test]
fn test_use_db() {
    let db_name = "hello";
    context_use_db(db_name);
    println!("current use db: {}", CONTEXT.lock().unwrap().db_name);
    println!("current use db: {}", CONTEXT.lock().unwrap().db_name);
    assert_eq!(db_name, CONTEXT.lock().unwrap().db_name);
}
