use ndb::parse;

#[test]
fn test_select_sql() {
    let sql = "select col,ab,names from mytable";
    parse::parse_sql(sql);
}

#[test]
fn test_create_table_sql() {
    let sql = "create table table_a(id bigint )";
    parse::parse_sql(sql);
}
