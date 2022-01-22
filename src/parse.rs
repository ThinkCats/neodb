use sqlparser::ast::{ColumnDef, Expr, Ident, Query, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

pub trait DDL {}

#[derive(Debug)]
struct SelectDef {
    columns: Vec<String>,
    table: Vec<String>,
}

#[derive(Debug)]
struct CreateTableDef {
    table_name: String,
    columns: Vec<ColumnDef>,
}

impl DDL for CreateTableDef {}

impl DDL for SelectDef {}

pub fn parse_sql(sql: &str) {
    let dialect = MySqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql);

    match ast {
        Ok(mut data) => {
            if data.len() > 1 {
                panic!("too much sql");
            }
            let statement = data.pop().unwrap();

            let ddl: Box<dyn DDL> = match statement {
                //process select query
                Statement::Query(q) => {
                    let select_def = parse_select_cols(q);
                    println!("query columns name:{:?}", select_def);
                    Box::new(select_def)
                }
                //process create table
                Statement::CreateTable {
                    name,
                    columns,
                    or_replace: _,
                    temporary: _,
                    constraints: _,
                    hive_distribution: _,
                    hive_formats: _,
                    with_options: _,
                    file_format: _,
                    table_properties: _,
                    location: _,
                    query: _,
                    without_rowid: _,
                    like: _,
                    external: _,
                    if_not_exists: _,
                } => {
                    let mut name_vec = name.0;
                    let table_name = name_vec.pop().unwrap().value;
                    let create_table_def = CreateTableDef {
                        table_name,
                        columns,
                    };
                    println!("create table def:{:?}", create_table_def);
                    Box::new(create_table_def)
                }
                Statement::Insert {
                    or: _,
                    table_name,
                    columns,
                    overwrite: _,
                    source: _,
                    partitioned: _,
                    after_columns: _,
                    table,
                    on: _,
                } => {
                    println!("table_name:{}", table_name);
                    panic!()
                }
                _ => {
                    panic!("body parse error")
                }
            };
        }
        Err(err) => {
            println!("find error:{:?}", err);
            panic!("parse error")
        }
    };
}

///parse select sql
fn parse_select_cols(q: Box<Query>) -> SelectDef {
    let mut cols: Vec<String> = Vec::new();
    let mut tables: Vec<String> = Vec::new();

    match q.body {
        SetExpr::Select(s) => {
            //columns parse
            for proj in s.projection {
                match proj {
                    SelectItem::UnnamedExpr(expr) => match expr {
                        Expr::Identifier(ident) => {
                            let value = ident.value;
                            cols.push(value);
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
            //table name parse,TODO join,alias
            for from in s.from {
                let relation = from.relation;
                match relation {
                    TableFactor::Table {
                        name,
                        alias: _,
                        args: _,
                        with_hints: _,
                    } => {
                        let names = name.0;
                        for ident in names {
                            tables.push(ident.value);
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    };

    SelectDef {
        columns: cols,
        table: tables,
    }
}
