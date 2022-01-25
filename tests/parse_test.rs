use ndb::parse;

#[test]
fn test_create_db() {
    let sql = "CREATE Schema hello;";
    let ddl = parse::parse_sql(sql);
    println!("create db ddl:{}", ddl.unwrap());
}

#[test]
fn test_select_sql() {
    let sql = "select col,ab,names from mytable";
    let ddl = parse::parse_sql(sql);
    println!("select ddl:{}", ddl.unwrap());
}

#[test]
fn test_create_table_sql() {
    let sql = "create table table_a(id bigint )";
    let ddl = parse::parse_sql(sql);
    println!("create table ddl:{}", ddl.unwrap());
}
